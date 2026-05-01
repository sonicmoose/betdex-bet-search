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
import java.util.LinkedHashMap;
import java.util.HashSet;
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
  private final Set<String> pendingMarketIds = ConcurrentHashMap.newKeySet();
  private final Map<String, Instant> cache = new ConcurrentHashMap<>();
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
      String marketId = price.marketId();
      if (marketId == null || marketId.isBlank() || isCached(marketId, now)) {
        continue;
      }
      pendingMarketIds.add(marketId);
    }

    if (!pendingMarketIds.isEmpty()) {
      scheduleFlush();
    }
  }

  private boolean isCached(String marketId, Instant now) {
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

    if (!pendingMarketIds.isEmpty()) {
      scheduleFlush();
    }
  }

  private void flush() {
    List<String> marketIds = drainBatch();
    if (marketIds.isEmpty()) {
      return;
    }

    List<Map<String, Object>> markets = fetchMarkets(marketIds);
    Set<String> foundIds = new HashSet<>();
    Instant now = Instant.now();

    for (Map<String, Object> market : markets) {
      String marketId = marketId(market);
      if (marketId == null) {
        continue;
      }
      foundIds.add(marketId);
      cache(marketId, now);
      openSearchWriter.enrichMarket(marketId, now, market);
    }

    for (String marketId : marketIds) {
      if (!foundIds.contains(marketId)) {
        cache(marketId, now);
      }
    }

    log.info("Enriched {} BetDEX markets from REST API requested={}", foundIds.size(), marketIds.size());
  }

  private List<String> drainBatch() {
    int batchSize = Math.max(1, properties.marketsBatchSize());
    List<String> batch = new ArrayList<>(batchSize);
    for (String marketId : pendingMarketIds) {
      if (batch.size() >= batchSize) {
        break;
      }
      if (pendingMarketIds.remove(marketId)) {
        batch.add(marketId);
      }
    }
    return batch;
  }

  private List<Map<String, Object>> fetchMarkets(List<String> marketIds) {
    String response = webClient.get()
        .uri(uriBuilder -> uriBuilder
            .path(properties.marketsPath())
            .queryParam(properties.marketsIdsParam(), String.join(",", marketIds))
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
    return node.isObject() ? List.of(objectMapper.convertValue(node, MAP_TYPE)) : List.of();
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
    for (String key : List.of("marketId", "id", "market_id")) {
      Object value = market.get(key);
      if (value != null && !value.toString().isBlank()) {
        return value.toString();
      }
    }
    return null;
  }

  private void cache(String marketId, Instant now) {
    cache.put(marketId, now.plus(properties.marketCacheTtl()));
  }

  @PreDestroy
  void stop() {
    scheduler.shutdownNow();
  }
}
