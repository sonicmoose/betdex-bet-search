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

    Map<String, Object> payload = Map.of(
        "size", maxResults,
        "_source", List.of("marketId", "raw.marketId"),
        "query", Map.of("term", Map.of("raw.status.keyword", "Open")));

    String response = postForResponse("/" + properties.marketsCurrentIndex() + "/_search", payload);
    try {
      JsonNode hits = objectMapper.readTree(response).path("hits").path("hits");
      Set<String> marketIds = new LinkedHashSet<>();
      if (hits.isArray()) {
        for (JsonNode hit : hits) {
          String marketId = firstText(hit.path("_source"), "marketId", "raw.marketId");
          if (marketId != null && !marketId.isBlank()) {
            marketIds.add(marketId);
          }
        }
      }
      return new ArrayList<>(marketIds);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to parse OpenSearch current market search response", e);
    }
  }

  public Map<String, Object> upsertMarket(String marketId, Instant receivedAt, Map<String, Object> payload) {
    if (marketId == null) {
      return Map.of();
    }
    Map<String, Object> document = currentDocument(marketId, receivedAt, payload);
    upsert(properties.marketsCurrentIndex(), marketId, document);
    return document;
  }

  public Map<String, Object> upsertMarketStatus(String marketId, Instant receivedAt, Map<String, Object> payload) {
    if (marketId == null) {
      return Map.of();
    }
    Map<String, Object> patch = currentDocument(marketId, receivedAt, payload);
    upsert(properties.marketsCurrentIndex(), marketId, patch);
    return patch;
  }

  public void enrichMarket(String marketId, Instant receivedAt, Map<String, Object> enrichment) {
    if (marketId == null || enrichment.isEmpty()) {
      return;
    }

    Map<String, Object> document = currentDocument(marketId, receivedAt, enrichment);
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
            "params", scriptParams(document, enrichment)),
        "upsert", document);
    post("/" + properties.marketsCurrentIndex() + "/_update/" + urlEncode(marketId), payload);
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

  public Map<String, Object> upsertMarketPrices(List<PriceUpdate> prices, Map<String, Object> enrichment) {
    if (prices.isEmpty() || prices.getFirst().marketId() == null) {
      return Map.of();
    }

    PriceUpdate first = prices.getFirst();
    Map<String, Object> document = currentPriceDocument(first, prices);
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
                if (ctx._source.raw == null) {
                  ctx._source.raw = params.patch.raw;
                } else if (params.patch.name != null || params.patch.eventName != null || params.outcomeNames != null) {
                  for (entry in params.patch.raw.entrySet()) {
                    if (entry.getKey() != 'status' || ctx._source.raw.status == null) {
                      ctx._source.raw[entry.getKey()] = entry.getValue();
                    }
                  }
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
                """,
            "params", scriptParams(document, enrichment)),
        "upsert", document);
    post("/" + properties.marketsCurrentIndex() + "/_update/" + urlEncode(first.marketId()), payload);
    return document;
  }

  private void applyTextEnrichment(Map<String, Object> document, Map<String, Object> enrichment) {
    if (enrichment == null || enrichment.isEmpty()) {
      return;
    }

    for (String key : List.of(
        "marketId",
        "eventId",
        "eventGroupId",
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
    Map<String, Object> params = new LinkedHashMap<>();
    params.put("patch", patch);
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

  private void upsert(String index, String id, Map<String, Object> document) {
    Map<String, Object> payload = Map.of("doc", document, "doc_as_upsert", true);
    post("/" + index + "/_update/" + urlEncode(id), payload);
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
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
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
    ContentStreamProvider content = () -> new ByteArrayInputStream(body);
    SdkHttpFullRequest request = SdkHttpFullRequest.builder()
        .method(SdkHttpMethod.fromValue(method))
        .protocol(endpoint.getScheme())
        .host(endpoint.getHost())
        .port(endpoint.getPort())
        .encodedPath(path)
        .putHeader("Content-Type", "application/json")
        .contentStreamProvider(content)
        .build();

    SdkHttpFullRequest signed = signer.sign(request, Aws4SignerParams.builder()
        .awsCredentials(credentialsProvider.resolveCredentials())
        .signingName("es")
        .signingRegion(region)
        .build());

    HttpRequest.Builder builder = HttpRequest.newBuilder(endpoint.resolve(path))
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
