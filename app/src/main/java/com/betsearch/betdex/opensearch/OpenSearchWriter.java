package com.betsearch.betdex.opensearch;

import com.betsearch.betdex.config.OpenSearchProperties;
import com.betsearch.betdex.ingest.PriceUpdate;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
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
    document.put("payload", payload);
    post("/" + dailyIndex(properties.rawAlias(), receivedAt) + "/_doc", document);
  }

  public void upsertMarket(String marketId, Instant receivedAt, Map<String, Object> payload) {
    if (marketId == null) {
      return;
    }
    upsert(properties.marketsCurrentIndex(), marketId, currentDocument(marketId, receivedAt, payload));
  }

  public void upsertMarketStatus(String marketId, Instant receivedAt, Map<String, Object> payload) {
    if (marketId == null) {
      return;
    }
    Map<String, Object> patch = currentDocument(marketId, receivedAt, payload);
    upsert(properties.marketsCurrentIndex(), marketId, patch);
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

  public void upsertMarketPrices(List<PriceUpdate> prices) {
    if (prices.isEmpty() || prices.getFirst().marketId() == null) {
      return;
    }

    PriceUpdate first = prices.getFirst();
    Map<String, Object> document = currentPriceDocument(first, prices);
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
                }
                """,
            "params", Map.of("patch", document)),
        "upsert", document);
    post("/" + properties.marketsCurrentIndex() + "/_update/" + urlEncode(first.marketId()), payload);
  }

  private Map<String, Object> currentDocument(String id, Instant receivedAt, Map<String, Object> payload) {
    Map<String, Object> document = new LinkedHashMap<>();
    document.put("id", id);
    document.put("marketId", payload.get("marketId"));
    document.put("eventId", payload.get("eventId"));
    document.put("categoryId", payload.get("categoryId"));
    document.put("subCategoryId", payload.get("subCategoryId"));
    document.put("receivedAt", receivedAt.toString());
    document.put("raw", payload);
    return document;
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
    document.put("latestPrices", latestPrices);
    document.put("latestPriceUpdateType", first.updateType());
    document.put("raw", raw);
    return document;
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

  private void requestWithRetry(String method, String path, byte[] body) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= 3; attempt++) {
      try {
        HttpResponse<String> response = httpClient.send(signedRequest(method, path, body), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 200 && response.statusCode() < 300) {
          return;
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
