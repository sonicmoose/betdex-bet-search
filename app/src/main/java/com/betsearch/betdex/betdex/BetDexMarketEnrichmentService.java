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
        continue;
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
    return markets;
  }

  private List<Map<String, Object>> fetchAndCacheMarketsByMarketIds(List<String> marketIds, Instant now) {
    List<Map<String, Object>> markets = fetchMarketsByMarketIds(marketIds).stream()
        .map(this::normalizeMarket)
        .toList();
    for (Map<String, Object> market : markets) {
      cacheMarket(market, now);
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
      result.addAll(markets);
      if (markets.size() < pageSize) {
        break;
      }
    }

    return result;
  }

  private List<Map<String, Object>> fetchMarketsByMarketIds(List<String> marketIds) {
    return fetchMarkets(properties.marketsIdsParam(), marketIds, 1, Math.max(1, properties.marketsPageSize()));
  }

  private List<Map<String, Object>> fetchMarkets(String idsParam, List<String> ids, int page, int pageSize) {
    String response = webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path(properties.marketsPath())
            .queryParam(idsParam, String.join(",", ids))
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

  private Map<String, Object> normalizeMarket(Map<String, Object> market) {
    Map<String, Object> normalized = new LinkedHashMap<>(market);
    putIfPresent(normalized, "marketId", marketId(market));
    putIfPresent(normalized, "eventId", eventId(market));
    putIfPresent(normalized, "eventGroupId", firstString(market, List.of("eventGroupId", "event_group_id", "leagueId", "league_id")));
    putIfPresent(normalized, "categoryId", firstString(market, List.of("categoryId", "category_id")));
    putIfPresent(normalized, "subCategoryId", firstString(market, List.of("subCategoryId", "sub_category_id", "sportId", "sport_id")));
    putIfPresent(normalized, "name", firstString(market, List.of("name", "marketName", "market_name")));
    putIfPresent(normalized, "eventName", eventName(market));
    Map<String, String> outcomeNames = outcomeNames(market);
    if (!outcomeNames.isEmpty()) {
      normalized.put("outcomeNames", outcomeNames);
      normalized.put("outcomeSearchText", String.join(" ", outcomeNames.values()));
    }
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
        if (!(value instanceof Map<?, ?> map)) {
          value = null;
          break;
        }
        value = map.get(segment);
      }
      if (value != null && !value.toString().isBlank()) {
        return value.toString();
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
      String name = firstString(map, List.of("outcomeName", "outcome_name", "name", "selectionName", "selection_name"));
      if (id != null && name != null) {
        names.put(id, name);
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

  private void cache(Map<String, Instant> cache, String marketId, Instant now) {
    cache.put(marketId, now.plus(properties.marketCacheTtl()));
  }

  private void cacheMarket(Map<String, Object> market, Instant now) {
    String marketId = marketId(market);
    if (marketId == null) {
      return;
    }
    marketDetailsCache.put(marketId, new LinkedHashMap<>(market));
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
