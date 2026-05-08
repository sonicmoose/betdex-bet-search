package com.betsearch.betdex.betdex;

import com.betsearch.betdex.config.BetDexProperties;
import com.betsearch.betdex.ingest.PriceUpdate;
import com.betsearch.betdex.opensearch.OpenSearchWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
public class BetDexMarketEnrichmentService {
  private static final Logger log = LoggerFactory.getLogger(BetDexMarketEnrichmentService.class);
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
  };

  private final BetDexProperties properties;
  private final BetDexSessionClient sessionClient;
  private final OpenSearchWriter openSearchWriter;
  private final ObjectMapper objectMapper;
  private final WebClient webClient;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final Set<String> pendingEventIds = ConcurrentHashMap.newKeySet();
  private final Set<String> pendingMarketIds = ConcurrentHashMap.newKeySet();
  private final Map<String, Instant> eventCache = new ConcurrentHashMap<>();
  private final Map<String, Instant> marketCache = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Object>> marketDetailsCache = new ConcurrentHashMap<>();
  private final AtomicReference<ScheduledFuture<?>> scheduledFlush = new AtomicReference<>();

  public BetDexMarketEnrichmentService(
      BetDexProperties properties,
      BetDexSessionClient sessionClient,
      OpenSearchWriter openSearchWriter,
      ObjectMapper objectMapper,
      WebClient.Builder webClientBuilder) {
    this.properties = properties;
    this.sessionClient = sessionClient;
    this.openSearchWriter = openSearchWriter;
    this.objectMapper = objectMapper;
    this.webClient = webClientBuilder.baseUrl(properties.restBaseUrl()).build();
  }

  public void requestMarketEnrichment(List<PriceUpdate> prices) {
    Instant now = Instant.now();
    for (PriceUpdate price : prices) {
      String eventId = price.eventId();
      if (eventId != null && !eventId.isBlank()) {
        if (!isCached(eventCache, eventId, now)) {
          pendingEventIds.add(eventId);
        }
      }

      String marketId = price.marketId();
      if (marketId != null && !marketId.isBlank() && !isCached(marketCache, marketId, now)) {
        pendingMarketIds.add(marketId);
      }
    }

    if (!pendingEventIds.isEmpty() || !pendingMarketIds.isEmpty()) {
      scheduleFlush();
    }
  }

  public void requestMarketEnrichment(JsonNode marketMessage) {
    String eventId = text(marketMessage, "eventId");
    String marketId = text(marketMessage, "marketId");
    Instant now = Instant.now();
    if (eventId != null && !eventId.isBlank() && !isCached(eventCache, eventId, now)) {
      pendingEventIds.add(eventId);
    }
    if (marketId != null && !marketId.isBlank() && !isCached(marketCache, marketId, now)) {
      pendingMarketIds.add(marketId);
    }
    if (!pendingEventIds.isEmpty() || !pendingMarketIds.isEmpty()) {
      scheduleFlush();
    }
  }

  public Map<String, Object> enrichmentForPrices(List<PriceUpdate> prices) {
    if (prices.isEmpty()) {
      return Map.of();
    }

    PriceUpdate first = prices.getFirst();
    Instant now = Instant.now();
    String marketId = first.marketId();
    if (marketId != null && !marketId.isBlank()) {
      Map<String, Object> cached = cachedMarket(marketId, now);
      if (!cached.isEmpty()) {
        return cached;
      }
    }

    String eventId = first.eventId();
    if (eventId != null && !eventId.isBlank() && !isCached(eventCache, eventId, now)) {
      try {
        List<Map<String, Object>> markets = fetchAndCacheMarketsByEventIds(List.of(eventId), now);
        Map<String, Object> match = matchingMarket(markets, marketId);
        if (!match.isEmpty()) {
          return match;
        }
      } catch (RuntimeException e) {
        log.warn("Failed to synchronously enrich BetDEX market from event REST lookup marketId={} eventId={}", marketId, eventId, e);
      }
    }

    if (marketId != null && !marketId.isBlank() && !isCached(marketCache, marketId, now)) {
      try {
        List<Map<String, Object>> markets = fetchAndCacheMarketsByMarketIds(List.of(marketId), now);
        Map<String, Object> match = matchingMarket(markets, marketId);
        if (!match.isEmpty()) {
          return match;
        }
      } catch (RuntimeException e) {
        log.warn("Failed to synchronously enrich BetDEX market from market REST lookup marketId={}", marketId, e);
      }
    }

    return Map.of();
  }

  public Map<String, Object> cachedEnrichmentForPrices(List<PriceUpdate> prices) {
    if (prices.isEmpty()) {
      return Map.of();
    }

    String marketId = prices.getFirst().marketId();
    if (marketId == null || marketId.isBlank()) {
      return Map.of();
    }
    return cachedMarket(marketId, Instant.now());
  }

  public int refreshMarketsByMarketIds(List<String> marketIds) {
    if (marketIds.isEmpty()) {
      return 0;
    }

    Instant now = Instant.now();
    List<Map<String, Object>> markets = fetchAndCacheMarketsByMarketIds(marketIds, now);
    Set<String> foundIds = new HashSet<>();
    int refreshed = 0;
    for (Map<String, Object> market : markets) {
      String marketId = marketId(market);
      if (marketId == null) {
        continue;
      }
      foundIds.add(marketId);
      openSearchWriter.enrichMarket(marketId, now, market);
      refreshed++;
    }
    int deletedMissing = 0;
    for (String marketId : marketIds) {
      if (!foundIds.contains(marketId)) {
        openSearchWriter.deleteMarket(marketId);
        deletedMissing++;
      }
    }
    log.info("Refreshed {} BetDEX markets from REST API requested={} deletedMissing={}", refreshed, marketIds.size(), deletedMissing);
    return refreshed;
  }

  private boolean isCached(Map<String, Instant> cache, String marketId, Instant now) {
    Instant expiresAt = cache.get(marketId);
    if (expiresAt == null) {
      return false;
    }
    if (expiresAt.isAfter(now)) {
      return true;
    }
    cache.remove(marketId, expiresAt);
    return false;
  }

  private void scheduleFlush() {
    if (scheduledFlush.get() != null) {
      return;
    }

    ScheduledFuture<?> task = scheduler.schedule(this::flushSafely, 250, TimeUnit.MILLISECONDS);
    if (!scheduledFlush.compareAndSet(null, task)) {
      task.cancel(false);
    }
  }

  private void flushSafely() {
    scheduledFlush.set(null);
    try {
      flush();
    } catch (RuntimeException e) {
      log.warn("Failed to enrich BetDEX markets from REST API", e);
    }

    if (!pendingEventIds.isEmpty() || !pendingMarketIds.isEmpty()) {
      scheduleFlush();
    }
  }

  private void flush() {
    List<String> eventIds = drainBatch(pendingEventIds);
    if (!eventIds.isEmpty()) {
      enrichByEventIds(eventIds);
      return;
    }

    List<String> marketIds = drainBatch(pendingMarketIds);
    if (marketIds.isEmpty()) {
      return;
    }

    enrichByMarketIds(marketIds);
  }

  private void enrichByEventIds(List<String> eventIds) {
    Instant now = Instant.now();
    List<Map<String, Object>> markets = fetchAndCacheMarketsByEventIds(eventIds, now);
    for (Map<String, Object> market : markets) {
      String marketId = marketId(market);
      if (marketId == null) {
        continue;
      }
      openSearchWriter.enrichMarket(marketId, now, market);
    }

    Set<String> enrichedEventIds = new HashSet<>();
    for (Map<String, Object> market : markets) {
      String eventId = eventId(market);
      if (eventId != null) {
        enrichedEventIds.add(eventId);
      }
    }
    for (String eventId : enrichedEventIds) {
      cache(eventCache, eventId, now);
    }

    log.info(
        "Enriched {} BetDEX markets from REST API eventIds={} matchedEventIds={}",
        markets.size(),
        eventIds.size(),
        enrichedEventIds.size());
  }

  private void enrichByMarketIds(List<String> marketIds) {
    Instant now = Instant.now();
    List<Map<String, Object>> markets = fetchAndCacheMarketsByMarketIds(marketIds, now);
    Set<String> foundIds = new HashSet<>();

    for (Map<String, Object> market : markets) {
      String marketId = marketId(market);
      if (marketId == null) {
        continue;
      }
      foundIds.add(marketId);
      openSearchWriter.enrichMarket(marketId, now, market);
    }

    for (String marketId : marketIds) {
      if (!foundIds.contains(marketId)) {
        cache(marketCache, marketId, now);
      }
    }

    log.info("Enriched {} BetDEX markets from REST API requested={}", foundIds.size(), marketIds.size());
  }

  private List<String> drainBatch(Set<String> pendingIds) {
    int batchSize = Math.max(1, properties.marketsBatchSize());
    List<String> batch = new ArrayList<>(batchSize);
    for (String marketId : pendingIds) {
      if (batch.size() >= batchSize) {
        break;
      }
      if (pendingIds.remove(marketId)) {
        batch.add(marketId);
      }
    }
    return batch;
  }

  private List<Map<String, Object>> fetchAndCacheMarketsByEventIds(List<String> eventIds, Instant now) {
    Set<String> requestedEventIds = new HashSet<>(eventIds);
    List<Map<String, Object>> markets = fetchMarketsByEventIds(eventIds).stream()
        .map(this::normalizeMarket)
        .filter(market -> {
          String eventId = eventId(market);
          return eventId != null && requestedEventIds.contains(eventId);
        })
        .toList();

    for (Map<String, Object> market : markets) {
      cacheMarket(market, now);
    }
    Set<String> enrichedEventIds = new HashSet<>();
    for (Map<String, Object> market : markets) {
      String eventId = eventId(market);
      if (eventId != null) {
        enrichedEventIds.add(eventId);
      }
    }
    for (String eventId : enrichedEventIds) {
      cache(eventCache, eventId, now);
    }
    if (markets.isEmpty()) {
      log.warn("BetDEX REST event lookup returned no matching markets eventIds={}", eventIds);
    }
    return markets;
  }

  private List<Map<String, Object>> fetchAndCacheMarketsByMarketIds(List<String> marketIds, Instant now) {
    List<Map<String, Object>> markets = fetchMarketsByMarketIds(marketIds).stream()
        .map(this::normalizeMarket)
        .toList();
    for (Map<String, Object> market : markets) {
      cacheMarket(market, now);
    }
    if (markets.isEmpty()) {
      log.warn("BetDEX REST market lookup returned no matching markets marketIds={}", marketIds);
    }
    return markets;
  }

  private Map<String, Object> matchingMarket(List<Map<String, Object>> markets, String marketId) {
    if (marketId == null || marketId.isBlank()) {
      return markets.isEmpty() ? Map.of() : markets.getFirst();
    }
    for (Map<String, Object> market : markets) {
      if (marketId.equals(marketId(market))) {
        return market;
      }
    }
    return Map.of();
  }

  private List<Map<String, Object>> fetchMarketsByEventIds(List<String> eventIds) {
    List<Map<String, Object>> result = new ArrayList<>();
    int maxPages = Math.max(1, properties.marketsMaxPages());
    int pageSize = Math.max(1, properties.marketsPageSize());
    int firstPage = properties.marketsFirstPage();

    for (int page = firstPage; page < firstPage + maxPages; page++) {
      List<Map<String, Object>> markets = fetchMarkets(properties.marketsEventIdsParam(), eventIds, page, pageSize);
      log.info("Fetched {} BetDEX markets from REST API by {} page={} requested={}", markets.size(), properties.marketsEventIdsParam(), page, eventIds.size());
      result.addAll(markets);
      if (markets.size() < pageSize) {
        break;
      }
    }

    return result;
  }

  private List<Map<String, Object>> fetchMarketsByMarketIds(List<String> marketIds) {
    List<Map<String, Object>> markets = fetchMarkets(properties.marketsIdsParam(), marketIds, properties.marketsFirstPage(), Math.max(1, properties.marketsPageSize()));
    log.info("Fetched {} BetDEX markets from REST API by {} requested={}", markets.size(), properties.marketsIdsParam(), marketIds.size());
    return markets;
  }

  private List<Map<String, Object>> fetchMarkets(String idsParam, List<String> ids, int page, int pageSize) {
    String response = webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path(properties.marketsPath())
            .queryParam(idsParam, ids.toArray())
            .queryParam(properties.marketsPageParam(), page)
            .queryParam(properties.marketsPageSizeParam(), pageSize)
            .build())
        .header(HttpHeaders.AUTHORIZATION, "Bearer " + sessionClient.accessToken())
        .retrieve()
        .onStatus(HttpStatusCode::isError, clientResponse ->
            clientResponse.bodyToMono(String.class)
                .defaultIfEmpty("")
                .flatMap(body -> Mono.error(new IllegalStateException(
                    "BetDEX markets request failed with HTTP " + clientResponse.statusCode().value()
                        + " from " + properties.restBaseUrl() + properties.marketsPath()
                        + ": " + body))))
        .bodyToMono(String.class)
        .block();

    if (response == null || response.isBlank()) {
      return List.of();
    }

    try {
      return marketList(objectMapper.readTree(response));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to parse BetDEX markets response", e);
    }
  }

  private List<Map<String, Object>> marketList(JsonNode node) {
    if (node == null || node.isNull()) {
      return List.of();
    }
    if (node.isArray()) {
      return arrayToMaps(node);
    }
    if (!node.isObject()) {
      return List.of();
    }
    JsonNode marketsNode = node.get("markets");
    if (marketsNode != null && marketsNode.isArray()) {
      return enrichPagedMarkets(node, marketsNode);
    }
    for (String field : List.of("markets", "items", "results", "data")) {
      JsonNode child = node.get(field);
      if (child != null) {
        if (child.isArray()) {
          return arrayToMaps(child);
        }
        if (child.isObject()) {
          List<Map<String, Object>> nested = marketList(child);
          if (!nested.isEmpty()) {
            return nested;
          }
          return List.of(objectMapper.convertValue(child, MAP_TYPE));
        }
      }
    }
    if (looksLikeMarket(node)) {
      return List.of(objectMapper.convertValue(node, MAP_TYPE));
    }
    List<Map<String, Object>> nested = new ArrayList<>();
    node.fields().forEachRemaining(entry -> nested.addAll(marketList(entry.getValue())));
    return nested;
  }

  private List<Map<String, Object>> enrichPagedMarkets(JsonNode root, JsonNode marketsNode) {
    Map<String, Map<String, Object>> events = mapsById(root.get("events"));
    Map<String, Map<String, Object>> eventGroups = mapsById(root.get("eventGroups"));
    Map<String, Map<String, Object>> subcategories = mapsById(root.get("subcategories"));
    Map<String, Map<String, Object>> categories = mapsById(root.get("categories"));
    Map<String, Map<String, Object>> marketOutcomes = mapsById(root.get("marketOutcomes"));

    List<Map<String, Object>> markets = arrayToMaps(marketsNode);
    for (Map<String, Object> market : markets) {
      String eventId = firstReferenceId(market.get("event"));
      Map<String, Object> event = eventId == null ? Map.of() : events.getOrDefault(eventId, Map.of());
      String eventGroupId = firstReferenceId(event.get("eventGroup"));
      Map<String, Object> eventGroup = eventGroupId == null ? Map.of() : eventGroups.getOrDefault(eventGroupId, Map.of());
      String subcategoryId = firstReferenceId(eventGroup.get("subcategory"));
      Map<String, Object> subcategory = subcategoryId == null ? Map.of() : subcategories.getOrDefault(subcategoryId, Map.of());
      String categoryId = firstReferenceId(subcategory.get("category"));
      Map<String, Object> category = categoryId == null ? Map.of() : categories.getOrDefault(categoryId, Map.of());

      putIfPresent(market, "marketId", firstString(market, List.of("id")));
      putIfPresent(market, "eventId", eventId);
      putIfPresent(market, "eventName", firstString(event, List.of("name")));
      putIfPresent(market, "eventGroupId", eventGroupId);
      putIfPresent(market, "eventGroupName", firstString(eventGroup, List.of("name")));
      putIfPresent(market, "subCategoryId", subcategoryId);
      putIfPresent(market, "subCategoryName", firstString(subcategory, List.of("name")));
      putIfPresent(market, "categoryId", categoryId);
      putIfPresent(market, "categoryName", firstString(category, List.of("name")));

      List<Map<String, Object>> outcomes = referencedMaps(market.get("marketOutcomes"), marketOutcomes);
      if (!outcomes.isEmpty()) {
        market.put("marketOutcomes", outcomes);
      }
    }
    return markets;
  }

  private Map<String, Map<String, Object>> mapsById(JsonNode node) {
    Map<String, Map<String, Object>> result = new LinkedHashMap<>();
    if (node == null || !node.isArray()) {
      return result;
    }
    for (Map<String, Object> item : arrayToMaps(node)) {
      String id = firstString(item, List.of("id"));
      if (id != null) {
        result.put(id, item);
      }
    }
    return result;
  }

  private List<Map<String, Object>> referencedMaps(Object reference, Map<String, Map<String, Object>> byId) {
    List<Map<String, Object>> result = new ArrayList<>();
    for (String id : referenceIds(reference)) {
      Map<String, Object> item = byId.get(id);
      if (item != null) {
        result.add(item);
      }
    }
    return result;
  }

  private boolean looksLikeMarket(JsonNode node) {
    return node.hasNonNull("marketId")
        || node.hasNonNull("market_id")
        || (node.hasNonNull("id") && (node.hasNonNull("name") || node.hasNonNull("marketName") || node.hasNonNull("market_name")));
  }

  private List<Map<String, Object>> arrayToMaps(JsonNode node) {
    List<Map<String, Object>> result = new ArrayList<>();
    for (JsonNode item : node) {
      if (item.isObject()) {
        result.add(new LinkedHashMap<>(objectMapper.convertValue(item, MAP_TYPE)));
      }
    }
    return result;
  }

  private String marketId(Map<String, Object> market) {
    return firstString(market, List.of("marketId", "market_id", "id"));
  }

  private String eventId(Map<String, Object> market) {
    String direct = firstString(market, List.of("eventId", "event_id"));
    if (direct != null) {
      return direct;
    }
    return firstNestedString(market, List.of(
        List.of("event", "eventId"),
        List.of("event", "event_id"),
        List.of("event", "id")));
  }

  private String marketTypeId(Map<String, Object> market) {
    String direct = firstString(market, List.of("marketTypeId", "market_type_id", "typeId", "type_id"));
    if (direct != null) {
      return direct;
    }
    return firstNestedString(market, List.of(
        List.of("marketType", "id"),
        List.of("market_type", "id"),
        List.of("marketTypes", "id"),
        List.of("market_types", "id"),
        List.of("type", "id")));
  }

  private Map<String, Object> normalizeMarket(Map<String, Object> market) {
    Map<String, Object> normalized = new LinkedHashMap<>(market);
    putIfPresent(normalized, "marketId", marketId(market));
    putIfPresent(normalized, "eventId", eventId(market));
    putIfPresent(normalized, "eventGroupId", firstString(market, List.of("eventGroupId", "event_group_id", "leagueId", "league_id")));
    putIfPresent(normalized, "eventGroupName", firstString(market, List.of("eventGroupName", "event_group_name", "leagueName", "league_name")));
    putIfPresent(normalized, "marketTypeId", marketTypeId(market));
    putIfPresent(normalized, "categoryId", firstString(market, List.of("categoryId", "category_id")));
    putIfPresent(normalized, "categoryName", firstString(market, List.of("categoryName", "category_name")));
    putIfPresent(normalized, "subCategoryId", firstString(market, List.of("subCategoryId", "sub_category_id", "sportId", "sport_id")));
    putIfPresent(normalized, "subCategoryName", firstString(market, List.of("subCategoryName", "sub_category_name", "sportName", "sport_name")));
    putIfPresent(normalized, "name", firstString(market, List.of("name", "marketName", "market_name", "displayName", "display_name", "title", "question")));
    putIfPresent(normalized, "eventName", eventName(market));
    putIfPresent(normalized, "enrichmentSearchText", searchText(market));
    Map<String, String> outcomeNames = outcomeNames(market);
    if (!outcomeNames.isEmpty()) {
      normalized.put("outcomeNames", outcomeNames);
      normalized.put("outcomeSearchText", String.join(" ", outcomeNames.values()));
    }
    log.info(
        "Normalized BetDEX market enrichment marketId={} eventId={} status={} inPlayStatus={} name={} eventName={} outcomeNames={}",
        normalized.get("marketId"),
        normalized.get("eventId"),
        normalized.get("status"),
        normalized.get("inPlayStatus"),
        normalized.get("name"),
        normalized.get("eventName"),
        outcomeNames.size());
    return normalized;
  }

  private void putIfPresent(Map<String, Object> target, String key, String value) {
    if (value != null && !value.isBlank()) {
      target.put(key, value);
    }
  }

  private String firstNestedString(Map<String, Object> source, List<List<String>> paths) {
    for (List<String> path : paths) {
      Object value = source;
      for (String segment : path) {
        value = nestedValue(value, segment);
        if (value == null) {
          break;
        }
      }
      if (value != null && !value.toString().isBlank()) {
        return value.toString();
      }
    }
    return null;
  }

  private Object nestedValue(Object value, String segment) {
    if (value instanceof Map<?, ?> map) {
      return map.get(segment);
    }
    if (value instanceof Iterable<?> iterable) {
      for (Object item : iterable) {
        Object nested = nestedValue(item, segment);
        if (nested != null && !nested.toString().isBlank()) {
          return nested;
        }
      }
    }
    return null;
  }

  private String eventName(Map<String, Object> market) {
    String direct = firstString(market, List.of("eventName", "event_name"));
    if (direct != null) {
      return direct;
    }
    return firstNestedString(market, List.of(
        List.of("event", "name"),
        List.of("event", "title"),
        List.of("event", "displayName"),
        List.of("event", "display_name"),
        List.of("event", "eventName"),
        List.of("event", "event_name")));
  }

  private Map<String, String> outcomeNames(Map<String, Object> market) {
    Map<String, String> names = new LinkedHashMap<>();
    for (String key : List.of("marketOutcomes", "outcomes", "selections", "runners")) {
      Object value = market.get(key);
      if (value instanceof List<?> list) {
        addOutcomeNames(names, list);
      }
    }
    return names;
  }

  private void addOutcomeNames(Map<String, String> names, List<?> outcomes) {
    for (Object outcome : outcomes) {
      if (!(outcome instanceof Map<?, ?> map)) {
        continue;
      }
      String id = firstString(map, List.of("outcomeId", "outcome_id", "id", "selectionId", "selection_id"));
      String name = firstString(map, List.of("outcomeName", "outcome_name", "name", "selectionName", "selection_name", "displayName", "display_name", "title", "runnerName", "runner_name", "label"));
      if (id != null && name != null) {
        names.put(id, name);
      }
    }
  }

  private String searchText(Object value) {
    List<String> values = new ArrayList<>();
    collectSearchText(value, values);
    return String.join(" ", values);
  }

  private void collectSearchText(Object value, List<String> values) {
    if (value == null) {
      return;
    }
    if (value instanceof String string && !string.isBlank()) {
      values.add(string);
      return;
    }
    if (value instanceof Number || value instanceof Boolean) {
      return;
    }
    if (value instanceof Map<?, ?> map) {
      for (Object nested : map.values()) {
        collectSearchText(nested, values);
      }
      return;
    }
    if (value instanceof Iterable<?> iterable) {
      for (Object nested : iterable) {
        collectSearchText(nested, values);
      }
    }
  }

  private String firstString(Map<?, ?> source, List<String> keys) {
    for (String key : keys) {
      Object value = source.get(key);
      if (value != null && !value.toString().isBlank()) {
        return value.toString();
      }
    }
    return null;
  }

  private String text(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }

  private String firstReferenceId(Object value) {
    List<String> ids = referenceIds(value);
    return ids.isEmpty() ? null : ids.getFirst();
  }

  private List<String> referenceIds(Object value) {
    if (!(value instanceof Map<?, ?> reference)) {
      return List.of();
    }
    Object ids = reference.get("_ids");
    if (ids instanceof List<?> list) {
      return list.stream()
          .filter(item -> item != null && !item.toString().isBlank())
          .map(Object::toString)
          .toList();
    }
    Object ref = reference.get("_ref");
    if (ref != null && !ref.toString().isBlank()) {
      return List.of(ref.toString());
    }
    Object id = reference.get("id");
    if (id != null && !id.toString().isBlank()) {
      return List.of(id.toString());
    }
    return List.of();
  }

  private void cache(Map<String, Instant> cache, String marketId, Instant now) {
    cache.put(marketId, now.plus(properties.marketCacheTtl()));
  }

  private void cacheMarket(Map<String, Object> market, Instant now) {
    String marketId = marketId(market);
    if (marketId == null) {
      return;
    }
    marketDetailsCache.put(marketId, new LinkedHashMap<>(market));
    pendingMarketIds.remove(marketId);
    cache(marketCache, marketId, now);
  }

  private Map<String, Object> cachedMarket(String marketId, Instant now) {
    if (!isCached(marketCache, marketId, now)) {
      marketDetailsCache.remove(marketId);
      return Map.of();
    }
    Map<String, Object> market = marketDetailsCache.get(marketId);
    return market == null ? Map.of() : market;
  }

  @PreDestroy
  void stop() {
    scheduler.shutdownNow();
  }
}
