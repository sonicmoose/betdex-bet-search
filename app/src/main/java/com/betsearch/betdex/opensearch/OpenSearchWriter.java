package com.betsearch.betdex.opensearch;

import com.betsearch.betdex.config.OpenSearchProperties;
import com.betsearch.betdex.ingest.PriceUpdate;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

@Component
public class OpenSearchWriter {
  private static final Logger log = LoggerFactory.getLogger(OpenSearchWriter.class);
  private static final DateTimeFormatter INDEX_DATE =
      DateTimeFormatter.ofPattern("yyyy.MM.dd").withLocale(Locale.ROOT).withZone(ZoneOffset.UTC);

  private final OpenSearchProperties properties;
  private final ObjectMapper objectMapper;
  private final AwsCredentialsProvider credentialsProvider;
  private final Region region;
  private final HttpClient httpClient = HttpClient.newHttpClient();
  private final Aws4Signer signer = Aws4Signer.create();
  private final AtomicBoolean disabledWarningLogged = new AtomicBoolean(false);

  public OpenSearchWriter(
      OpenSearchProperties properties,
      ObjectMapper objectMapper,
      AwsCredentialsProvider credentialsProvider,
      Region region) {
    this.properties = properties;
    this.objectMapper = objectMapper;
    this.credentialsProvider = credentialsProvider;
    this.region = region;
  }

  public void indexRaw(
      Instant receivedAt,
      String messageType,
      String marketId,
      String eventId,
      Map<String, Object> payload) {
    Map<String, Object> document = new LinkedHashMap<>();
    document.put("receivedAt", receivedAt.toString());
    document.put("messageType", messageType);
    document.put("marketId", marketId);
    document.put("eventId", eventId);
    document.put("payloadJson", jsonString(payload));
    post("/" + dailyIndex(properties.rawAlias(), receivedAt) + "/_doc", document);
  }

  public List<String> openMarketIds(int maxResults) {
    if (maxResults <= 0 || properties.endpoint() == null || properties.endpoint().isBlank()) {
      return List.of();
    }

    int pageSize = Math.min(500, maxResults);
    Set<String> marketIds = new LinkedHashSet<>();
    List<String> searchAfter = null;
    while (marketIds.size() < maxResults) {
      Map<String, Object> payload = new LinkedHashMap<>();
      payload.put("size", Math.min(pageSize, maxResults - marketIds.size()));
      payload.put("_source", List.of("marketId", "raw.marketId"));
      payload.put("sort", List.of(Map.of("_id", Map.of("order", "asc"))));
      payload.put("query", Map.of("term", Map.of("raw.status.keyword", "Open")));
      if (searchAfter != null) {
        payload.put("search_after", searchAfter);
      }

      String response = postForResponse("/" + properties.marketsCurrentIndex() + "/_search", payload);
      try {
        JsonNode hits = objectMapper.readTree(response).path("hits").path("hits");
        if (!hits.isArray() || hits.isEmpty()) {
          break;
        }
        JsonNode lastSort = null;
        for (JsonNode hit : hits) {
          String marketId = firstText(hit.path("_source"), "marketId", "raw.marketId");
          if (marketId != null && !marketId.isBlank()) {
            marketIds.add(marketId);
          }
          lastSort = hit.path("sort");
        }
        searchAfter = sortValues(lastSort);
        if (searchAfter.isEmpty()) {
          break;
        }
      } catch (JsonProcessingException e) {
        throw new IllegalStateException("Failed to parse OpenSearch current market search response", e);
      }
    }
    return new ArrayList<>(marketIds);
  }

  private List<String> sortValues(JsonNode sort) {
    if (sort == null || !sort.isArray()) {
      return List.of();
    }
    List<String> values = new ArrayList<>();
    for (JsonNode value : sort) {
      values.add(value.asText());
    }
    return values;
  }

  public Map<String, Object> upsertMarket(String marketId, Instant receivedAt, Map<String, Object> payload) {
    if (marketId == null) {
      return Map.of();
    }
    if (isNonOpen(payload)) {
      deleteMarket(marketId);
      return currentDocument(marketId, receivedAt, payload);
    }
    Map<String, Object> document = currentDocument(marketId, receivedAt, payload);
    if (clearsPrices(payload)) {
      clearPrices(document);
      mergeCurrentMarket(marketId, document, true);
    } else {
      upsert(properties.marketsCurrentIndex(), marketId, document);
    }
    return document;
  }

  public Map<String, Object> upsertMarketStatus(String marketId, Instant receivedAt, Map<String, Object> payload) {
    if (marketId == null) {
      return Map.of();
    }
    Map<String, Object> patch = currentDocument(marketId, receivedAt, payload);
    if (isNonOpen(payload)) {
      deleteMarket(marketId);
      return patch;
    }
    if (clearsPrices(payload)) {
      clearPrices(patch);
      mergeCurrentMarket(marketId, patch, true);
    } else {
      mergeCurrentMarket(marketId, patch, false);
    }
    return patch;
  }

  public void enrichMarket(String marketId, Instant receivedAt, Map<String, Object> enrichment) {
    if (marketId == null || enrichment.isEmpty()) {
      return;
    }
    if (isNonOpen(enrichment)) {
      deleteMarket(marketId);
      return;
    }

    Map<String, Object> document = currentDocument(marketId, receivedAt, enrichment);
    boolean clearPrices = clearsPrices(enrichment);
    if (clearPrices) {
      clearPrices(document);
    }
    Map<String, Object> payload = Map.of(
        "script", Map.of(
            "lang", "painless",
            "source", """
                for (entry in params.patch.entrySet()) {
                  if (entry.getKey() != 'raw') {
                    ctx._source[entry.getKey()] = entry.getValue();
                  }
                }
                if (ctx._source.raw == null) {
                  ctx._source.raw = [:];
                }
                for (entry in params.patch.raw.entrySet()) {
                  ctx._source.raw[entry.getKey()] = entry.getValue();
                }
                if (params.clearPrices) {
                  ctx._source.latestPrices = [];
                  ctx._source.liquidity = 0;
                  ctx._source.raw.marketOutcomes = [];
                  ctx._source.raw.liquidity = 0;
                }
                if (params.outcomeNames != null) {
                  if (ctx._source.latestPrices != null) {
                    for (price in ctx._source.latestPrices) {
                      def name = params.outcomeNames[price.outcomeId];
                      if (name != null) {
                        price.name = name;
                        price.outcomeName = name;
                      }
                    }
                  }
                  if (ctx._source.raw.marketOutcomes != null) {
                    for (price in ctx._source.raw.marketOutcomes) {
                      def name = params.outcomeNames[price.outcomeId];
                      if (name != null) {
                        price.name = name;
                        price.outcomeName = name;
                      }
                    }
                  }
                }
                """,
            "params", scriptParams(document, enrichment, clearPrices)),
        "upsert", document);
    post(updatePath(properties.marketsCurrentIndex(), marketId), payload);
  }

  public void upsertEvent(String eventId, Instant receivedAt, Map<String, Object> payload) {
    if (eventId == null) {
      return;
    }
    Map<String, Object> document = currentDocument(eventId, receivedAt, payload);
    document.put("eventId", eventId);
    upsert(properties.eventsCurrentIndex(), eventId, document);
  }

  public void indexPrice(PriceUpdate price) {
    post("/" + dailyIndex(properties.pricesAlias(), price.receivedAt()) + "/_doc", price.source());
  }

  public void indexPrice(PriceUpdate price, Map<String, Object> enrichment) {
    Map<String, Object> document = new LinkedHashMap<>(price.source());
    applyTextEnrichment(document, enrichment);
    post("/" + dailyIndex(properties.pricesAlias(), price.receivedAt()) + "/_doc", document);
  }

  public Map<String, Object> upsertMarketPrices(List<PriceUpdate> prices) {
    return upsertMarketPrices(prices, Map.of());
  }

  public Map<String, Object> clearMarketPrices(JsonNode root, Instant receivedAt, Map<String, Object> enrichment) {
    String marketId = firstText(root, "marketId", "market_id", "marketID");
    if (marketId == null) {
      return Map.of();
    }
    if (isNonOpen(enrichment)) {
      deleteMarket(marketId);
      return currentDocument(marketId, receivedAt, enrichment);
    }

    Map<String, Object> document = currentEmptyPriceDocument(root, receivedAt);
    applyTextEnrichment(document, enrichment);
    Map<String, Object> payload = Map.of(
        "script", Map.of(
            "lang", "painless",
            "source", """
                for (entry in params.patch.entrySet()) {
                  if (entry.getKey() != 'raw') {
                    ctx._source[entry.getKey()] = entry.getValue();
                  }
                }
                ctx._source.latestPrices = [];
                ctx._source.liquidity = 0;
                if (ctx._source.raw == null) {
                  ctx._source.raw = params.patch.raw;
                } else {
                  for (entry in params.patch.raw.entrySet()) {
                    if (entry.getKey() != 'status' || ctx._source.raw.status == null || entry.getValue() != 'Open') {
                      ctx._source.raw[entry.getKey()] = entry.getValue();
                    }
                  }
                  ctx._source.raw.marketOutcomes = [];
                  ctx._source.raw.liquidity = 0;
                }
                """,
            "params", scriptParams(document, enrichment)),
        "upsert", document);
    post(updatePath(properties.marketsCurrentIndex(), marketId), payload);
    return document;
  }

  public Map<String, Object> upsertMarketPrices(List<PriceUpdate> prices, Map<String, Object> enrichment) {
    if (prices.isEmpty() || prices.getFirst().marketId() == null) {
      return Map.of();
    }

    PriceUpdate first = prices.getFirst();
    if (isNonOpen(enrichment)) {
      deleteMarket(first.marketId());
      return currentDocument(first.marketId(), first.receivedAt(), enrichment);
    }

    Map<String, Object> document = currentPriceDocument(first, prices);
    applyTextEnrichment(document, enrichment);
    Map<String, Object> payload = Map.of(
        "script", Map.of(
            "lang", "painless",
            "source", """
                boolean incremental = params.patch.latestPriceUpdateType == 'Incremental';
                for (entry in params.patch.entrySet()) {
                  if (entry.getKey() != 'raw') {
                    if (incremental && (entry.getKey() == 'latestPrices' || entry.getKey() == 'liquidity')) {
                      continue;
                    }
                    ctx._source[entry.getKey()] = entry.getValue();
                  }
                }
                if (incremental) {
                  if (ctx._source.latestPrices == null) {
                    ctx._source.latestPrices = [];
                  }
                  for (patchPrice in params.patch.latestPrices) {
                    for (int i = ctx._source.latestPrices.size() - 1; i >= 0; i--) {
                      def current = ctx._source.latestPrices[i];
                      boolean sameOutcome = current.outcomeId != null && patchPrice.outcomeId != null
                          && current.outcomeId.toString().equals(patchPrice.outcomeId.toString());
                      boolean sameSide = current.side != null && patchPrice.side != null
                          && current.side.toString().equals(patchPrice.side.toString());
                      boolean samePrice = current.price != null && patchPrice.price != null
                          && Double.parseDouble(current.price.toString()) == Double.parseDouble(patchPrice.price.toString());
                      if (sameOutcome && sameSide && samePrice) {
                        ctx._source.latestPrices.remove(i);
                      }
                    }
                    if (patchPrice.liquidity != null && patchPrice.liquidity > 0) {
                      ctx._source.latestPrices.add(patchPrice);
                    }
                  }
                  double totalLiquidity = 0;
                  for (price in ctx._source.latestPrices) {
                    if (price.liquidity != null) {
                      totalLiquidity += price.liquidity;
                    }
                  }
                  ctx._source.liquidity = totalLiquidity;
                }
                if (ctx._source.raw == null) {
                  ctx._source.raw = [:];
                }
                for (entry in params.patch.raw.entrySet()) {
                  if (entry.getKey() != 'marketOutcomes' && entry.getKey() != 'liquidity') {
                    if (entry.getKey() != 'status' || ctx._source.raw.status == null || entry.getValue() != 'Open') {
                      ctx._source.raw[entry.getKey()] = entry.getValue();
                    }
                  }
                }
                if (params.patch.name != null || params.patch.eventName != null || params.outcomeNames != null) {
                  if (params.outcomeNames != null && ctx._source.raw.marketOutcomes != null) {
                    for (price in ctx._source.raw.marketOutcomes) {
                      def name = params.outcomeNames[price.outcomeId];
                      if (name != null) {
                        price.name = name;
                        price.outcomeName = name;
                      }
                    }
                  }
                }
                ctx._source.raw.marketOutcomes = ctx._source.latestPrices == null ? [] : ctx._source.latestPrices;
                ctx._source.raw.liquidity = ctx._source.liquidity == null ? 0 : ctx._source.liquidity;
                ctx._source.raw.latestPriceUpdateType = params.patch.latestPriceUpdateType;
                """,
            "params", scriptParams(document, enrichment)),
        "upsert", document);
    String response = postForResponse(updatePath(properties.marketsCurrentIndex(), first.marketId()), payload);
    log.info(
        "Updated OpenSearch market prices marketId={} updateType={} priceCount={} firstPrice={} result={}",
        first.marketId(),
        first.updateType(),
        prices.size(),
        firstPriceSummary(prices),
        updateResult(response));
    return document;
  }

  public void deleteMarket(String marketId) {
    if (marketId == null || marketId.isBlank()) {
      return;
    }
    requestWithRetry("DELETE", "/" + properties.marketsCurrentIndex() + "/_doc/" + urlEncode(marketId), new byte[0]);
  }

  private boolean isNonOpen(Map<String, Object> payload) {
    Object status = payload == null ? null : payload.get("status");
    return status != null && !"Open".equalsIgnoreCase(status.toString());
  }

  private boolean clearsPrices(Map<String, Object> payload) {
    Object inPlayStatus = payload == null ? null : payload.get("inPlayStatus");
    return inPlayStatus != null && "InPlay".equalsIgnoreCase(inPlayStatus.toString());
  }

  private void clearPrices(Map<String, Object> document) {
    document.put("latestPrices", List.of());
    document.put("liquidity", BigDecimal.ZERO);
    Object rawValue = document.get("raw");
    if (rawValue instanceof Map<?, ?> rawMap) {
      @SuppressWarnings("unchecked")
      Map<String, Object> raw = (Map<String, Object>) rawMap;
      raw.put("marketOutcomes", List.of());
      raw.put("liquidity", BigDecimal.ZERO);
    }
  }

  private void mergeCurrentMarket(String marketId, Map<String, Object> document, boolean clearPrices) {
    Map<String, Object> payload = Map.of(
        "script", Map.of(
            "lang", "painless",
            "source", """
                for (entry in params.patch.entrySet()) {
                  if (entry.getKey() != 'raw') {
                    ctx._source[entry.getKey()] = entry.getValue();
                  }
                }
                if (ctx._source.raw == null) {
                  ctx._source.raw = [:];
                }
                for (entry in params.patch.raw.entrySet()) {
                  ctx._source.raw[entry.getKey()] = entry.getValue();
                }
                if (params.clearPrices) {
                  ctx._source.latestPrices = [];
                  ctx._source.liquidity = 0;
                  ctx._source.raw.marketOutcomes = [];
                  ctx._source.raw.liquidity = 0;
                }
                """,
            "params", scriptParams(document, Map.of(), clearPrices)),
        "upsert", document);
    post(updatePath(properties.marketsCurrentIndex(), marketId), payload);
  }

  private void applyTextEnrichment(Map<String, Object> document, Map<String, Object> enrichment) {
    if (enrichment == null || enrichment.isEmpty()) {
      return;
    }

    for (String key : List.of(
        "marketId",
        "eventId",
        "eventGroupId",
        "marketTypeId",
        "categoryId",
        "categoryName",
        "subCategoryId",
        "subCategoryName",
        "name",
        "eventName",
        "outcomeNameItems",
        "outcomeSearchText",
        "enrichmentSearchText")) {
      Object value = enrichment.get(key);
      if (value != null) {
        document.put(key, value);
      }
    }

    Object rawValue = document.get("raw");
    if (rawValue instanceof Map<?, ?> rawMap) {
      @SuppressWarnings("unchecked")
      Map<String, Object> raw = (Map<String, Object>) rawMap;
      canonicalRaw(enrichment).forEach(raw::put);
    }

    Object outcomeNames = enrichment.get("outcomeNames");
    if (outcomeNames != null) {
      document.put("outcomeNameItems", outcomeNameItems(outcomeNames));
    }
    applyOutcomeNames(document, enrichment);
  }

  private void applyOutcomeNames(Map<String, Object> document, Map<String, Object> enrichment) {
    Object namesValue = enrichment.get("outcomeNames");
    if (!(namesValue instanceof Map<?, ?> names)) {
      return;
    }
    Object latestPrices = document.get("latestPrices");
    if (latestPrices instanceof List<?> prices) {
      applyOutcomeNames(prices, names);
    }
    Object rawValue = document.get("raw");
    if (rawValue instanceof Map<?, ?> raw) {
      Object marketOutcomes = raw.get("marketOutcomes");
      if (marketOutcomes instanceof List<?> prices) {
        applyOutcomeNames(prices, names);
      }
    }
  }

  private void applyOutcomeNames(List<?> prices, Map<?, ?> names) {
    for (Object price : prices) {
      if (!(price instanceof Map<?, ?> priceMap)) {
        continue;
      }
      Object outcomeId = priceMap.get("outcomeId");
      Object name = names.get(outcomeId);
      if (outcomeId == null || name == null) {
        continue;
      }
      @SuppressWarnings("unchecked")
      Map<String, Object> mutablePrice = (Map<String, Object>) priceMap;
      mutablePrice.put("name", name);
      mutablePrice.put("outcomeName", name);
    }
  }

  private Map<String, Object> currentDocument(String id, Instant receivedAt, Map<String, Object> payload) {
    Map<String, Object> document = new LinkedHashMap<>();
    document.put("id", id);
    document.put("marketId", firstValue(payload, "marketId", "id"));
    document.put("eventId", payload.get("eventId"));
    document.put("eventGroupId", payload.get("eventGroupId"));
    document.put("marketTypeId", payload.get("marketTypeId"));
    document.put("categoryId", payload.get("categoryId"));
    document.put("subCategoryId", payload.get("subCategoryId"));
    document.put("name", payload.get("name"));
    document.put("eventName", payload.get("eventName"));
    document.put("outcomeNameItems", outcomeNameItems(payload.get("outcomeNames")));
    document.put("outcomeSearchText", payload.get("outcomeSearchText"));
    document.put("enrichmentSearchText", payload.get("enrichmentSearchText"));
    document.put("receivedAt", receivedAt.toString());
    Map<String, Object> raw = canonicalRaw(payload);
    if (!raw.containsKey("marketId") && raw.containsKey("id")) {
      raw.put("marketId", raw.get("id"));
    }
    document.put("raw", raw);
    document.entrySet().removeIf(entry -> entry.getValue() == null);
    return document;
  }

  private Object firstValue(Map<String, Object> payload, String... keys) {
    for (String key : keys) {
      Object value = payload.get(key);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private Map<String, Object> canonicalRaw(Map<String, Object> payload) {
    Map<String, Object> raw = new LinkedHashMap<>();
    for (String key : List.of(
        "id",
        "marketId",
        "eventId",
        "eventGroupId",
        "eventGroupName",
        "marketTypeId",
        "market_type_id",
        "categoryId",
        "categoryName",
        "subCategoryId",
        "subCategoryName",
        "name",
        "eventName",
        "status",
        "inPlayStatus",
        "lockAt",
        "expectedStartTime",
        "matched",
        "totalMatched",
        "liquidity",
        "outcomeNameItems",
        "outcomeSearchText",
        "enrichmentSearchText",
        "latestPriceUpdateType")) {
      Object value = payload.get(key);
      if (value != null) {
        raw.put(key, value);
      }
    }
    return raw;
  }

  private Map<String, Object> scriptParams(Map<String, Object> patch, Map<String, Object> enrichment) {
    return scriptParams(patch, enrichment, false);
  }

  private Map<String, Object> scriptParams(Map<String, Object> patch, Map<String, Object> enrichment, boolean clearPrices) {
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("patch", patch);
    params.put("clearPrices", clearPrices);
    Object outcomeNames = enrichment == null ? null : enrichment.get("outcomeNames");
    if (outcomeNames != null) {
      params.put("outcomeNames", outcomeNames);
    }
    return params;
  }

  private List<Map<String, Object>> outcomeNameItems(Object outcomeNames) {
    if (!(outcomeNames instanceof Map<?, ?> names)) {
      return List.of();
    }
    return names.entrySet().stream()
        .map(entry -> {
          Map<String, Object> item = new LinkedHashMap<>();
          item.put("outcomeId", entry.getKey());
          item.put("name", entry.getValue());
          return item;
        })
        .toList();
  }

  private Map<String, Object> currentPriceDocument(PriceUpdate first, List<PriceUpdate> prices) {
    List<Map<String, Object>> latestPrices = prices.stream()
        .map(this::latestPriceDocument)
        .toList();

    Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("marketId", first.marketId());
    raw.put("eventId", first.eventId());
    raw.put("eventGroupId", first.eventGroupId());
    raw.put("categoryId", first.categoryId());
    raw.put("categoryName", first.categoryId());
    raw.put("subCategoryId", first.subCategoryId());
    raw.put("subCategoryName", first.subCategoryId());
    raw.put("name", first.marketId());
    raw.put("eventName", first.eventId() == null ? first.marketId() : first.eventId());
    raw.put("status", "Open");
    raw.put("inPlayStatus", "NotApplicable");
    raw.put("liquidity", totalLiquidity(prices));
    raw.put("marketOutcomes", latestPrices);
    raw.put("latestPriceUpdateType", first.updateType());

    Map<String, Object> document = new LinkedHashMap<>();
    document.put("id", first.marketId());
    document.put("marketId", first.marketId());
    document.put("eventId", first.eventId());
    document.put("eventGroupId", first.eventGroupId());
    document.put("categoryId", first.categoryId());
    document.put("subCategoryId", first.subCategoryId());
    document.put("receivedAt", first.receivedAt().toString());
    document.put("liquidity", totalLiquidity(prices));
    document.put("latestPrices", latestPrices);
    document.put("latestPriceUpdateType", first.updateType());
    document.put("raw", raw);
    return document;
  }

  private Map<String, Object> currentEmptyPriceDocument(JsonNode root, Instant receivedAt) {
    String marketId = firstText(root, "marketId", "market_id", "marketID");
    String eventId = firstText(root, "eventId", "event_id", "eventID");

    Map<String, Object> raw = new LinkedHashMap<>();
    raw.put("marketId", marketId);
    raw.put("eventId", eventId);
    raw.put("eventGroupId", text(root, "eventGroupId"));
    raw.put("categoryId", text(root, "categoryId"));
    raw.put("categoryName", text(root, "categoryId"));
    raw.put("subCategoryId", text(root, "subCategoryId"));
    raw.put("subCategoryName", text(root, "subCategoryId"));
    raw.put("name", marketId);
    raw.put("eventName", eventId == null ? marketId : eventId);
    raw.put("status", "Open");
    raw.put("inPlayStatus", "NotApplicable");
    raw.put("liquidity", BigDecimal.ZERO);
    raw.put("marketOutcomes", List.of());
    raw.put("latestPriceUpdateType", text(root, "updateType"));

    Map<String, Object> document = new LinkedHashMap<>();
    document.put("id", marketId);
    document.put("marketId", marketId);
    document.put("eventId", eventId);
    document.put("eventGroupId", text(root, "eventGroupId"));
    document.put("categoryId", text(root, "categoryId"));
    document.put("subCategoryId", text(root, "subCategoryId"));
    document.put("receivedAt", receivedAt.toString());
    document.put("liquidity", BigDecimal.ZERO);
    document.put("latestPrices", List.of());
    document.put("latestPriceUpdateType", text(root, "updateType"));
    document.put("raw", raw);
    return document;
  }

  private BigDecimal totalLiquidity(List<PriceUpdate> prices) {
    return prices.stream()
        .map(PriceUpdate::liquidity)
        .filter(value -> value != null)
        .reduce(BigDecimal.ZERO, BigDecimal::add);
  }

  private Map<String, Object> latestPriceDocument(PriceUpdate price) {
    Map<String, Object> document = new LinkedHashMap<>();
    document.put("outcomeId", price.outcomeId());
    document.put("id", price.outcomeId());
    document.put("name", price.outcomeId());
    document.put("outcomeName", price.outcomeId());
    document.put("side", price.side());
    document.put("currencyId", price.currencyId());
    document.put("price", price.price());
    document.put("liquidity", price.liquidity());
    document.put("change", price.change());
    document.put("validAt", (price.validAt() == null ? price.receivedAt() : price.validAt()).toString());
    return document;
  }

  private String text(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }

  private void upsert(String index, String id, Map<String, Object> document) {
    Map<String, Object> payload = Map.of("doc", document, "doc_as_upsert", true);
    post(updatePath(index, id), payload);
  }

  private String updatePath(String index, String id) {
    return "/" + index + "/_update/" + urlEncode(id) + "?retry_on_conflict=10";
  }

  private String firstPriceSummary(List<PriceUpdate> prices) {
    if (prices.isEmpty()) {
      return null;
    }
    PriceUpdate price = prices.getFirst();
    return "outcomeId=" + price.outcomeId()
        + ",side=" + price.side()
        + ",price=" + price.price()
        + ",liquidity=" + price.liquidity()
        + ",validAt=" + price.validAt();
  }

  private String updateResult(String response) {
    try {
      JsonNode body = objectMapper.readTree(response);
      return "result=" + body.path("result").asText()
          + ",seqNo=" + body.path("_seq_no").asText()
          + ",shards=" + body.path("_shards").toString();
    } catch (JsonProcessingException e) {
      return response;
    }
  }

  private void post(String path, Map<String, Object> document) {
    if (properties.endpoint() == null || properties.endpoint().isBlank()) {
      if (disabledWarningLogged.compareAndSet(false, true)) {
        log.warn("OpenSearch indexing is disabled because OPENSEARCH_ENDPOINT is blank");
      }
      return;
    }
    byte[] body = json(document);
    requestWithRetry("POST", path, body);
  }

  private String postForResponse(String path, Map<String, Object> document) {
    byte[] body = json(document);
    return requestWithRetry("POST", path, body);
  }

  private String requestWithRetry(String method, String path, byte[] body) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= 3; attempt++) {
      try {
        HttpResponse<String> response = httpClient.send(signedRequest(method, path, body), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300 || "DELETE".equals(method) && response.statusCode() == 404) {
          return response.body();
        }
        last = new IllegalStateException("OpenSearch returned " + response.statusCode() + ": " + response.body());
      } catch (IOException e) {
        last = new IllegalStateException("OpenSearch request failed", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted during OpenSearch request", e);
      }
      sleepBackoff(attempt);
    }
    throw last;
  }

  private String firstText(JsonNode source, String... paths) {
    for (String path : paths) {
      JsonNode current = source;
      for (String segment : path.split("\\.")) {
        current = current.path(segment);
      }
      if (!current.isMissingNode() && !current.isNull() && !current.asText().isBlank()) {
        return current.asText();
      }
    }
    return null;
  }

  private HttpRequest signedRequest(String method, String path, byte[] body) {
    URI endpoint = URI.create(properties.endpoint());
    URI requestUri = endpoint.resolve(path);
    ContentStreamProvider content = () -> new ByteArrayInputStream(body);
    SdkHttpFullRequest.Builder requestBuilder = SdkHttpFullRequest.builder()
        .method(SdkHttpMethod.fromValue(method))
        .protocol(endpoint.getScheme())
        .host(endpoint.getHost())
        .port(endpoint.getPort())
        .encodedPath(requestUri.getRawPath())
        .putHeader("Content-Type", "application/json")
        .contentStreamProvider(content);
    if (requestUri.getRawQuery() != null && !requestUri.getRawQuery().isBlank()) {
      for (String pair : requestUri.getRawQuery().split("&")) {
        String[] parts = pair.split("=", 2);
        requestBuilder.appendRawQueryParameter(parts[0], parts.length > 1 ? parts[1] : null);
      }
    }
    SdkHttpFullRequest request = requestBuilder.build();

    SdkHttpFullRequest signed = signer.sign(request, Aws4SignerParams.builder()
        .awsCredentials(credentialsProvider.resolveCredentials())
        .signingName("es")
        .signingRegion(region)
        .build());

    HttpRequest.Builder builder = HttpRequest.newBuilder(requestUri)
        .method(method, HttpRequest.BodyPublishers.ofByteArray(body));
    signed.headers().forEach((name, values) -> {
      if (!"host".equalsIgnoreCase(name)) {
        values.forEach(value -> builder.header(name, value));
      }
    });
    return builder.build();
  }

  private byte[] json(Map<String, Object> document) {
    try {
      return objectMapper.writeValueAsBytes(document);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize OpenSearch document", e);
    }
  }

  private String jsonString(Map<String, Object> document) {
    try {
      return objectMapper.writeValueAsString(document);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to serialize OpenSearch document", e);
    }
  }

  private String dailyIndex(String aliasPrefix, Instant instant) {
    return aliasPrefix + "-" + INDEX_DATE.format(instant);
  }

  private String urlEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private void sleepBackoff(int attempt) {
    try {
      Thread.sleep(250L * attempt * attempt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted during OpenSearch retry backoff", e);
    }
  }
}
