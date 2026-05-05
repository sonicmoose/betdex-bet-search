package com.betsearch.betdex.appsync;

import com.betsearch.betdex.config.AppSyncProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
public class AppSyncMarketUpdatePublisher {
  private static final Logger log = LoggerFactory.getLogger(AppSyncMarketUpdatePublisher.class);
  private static final String MUTATION = """
      mutation PublishMarketUpdated($input: MarketUpdatedInput!) {
        publishMarketUpdated(input: $input) {
          marketId
          eventId
          updateType
          receivedAt
          source
        }
      }
      """;

  private final AppSyncProperties properties;
  private final ObjectMapper objectMapper;
  private final WebClient webClient;
  private final BlockingQueue<PublishRequest> queue;
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicInteger droppedSinceLastLog = new AtomicInteger();

  public AppSyncMarketUpdatePublisher(
      AppSyncProperties properties,
      ObjectMapper objectMapper,
      WebClient.Builder webClientBuilder) {
    this.properties = properties;
    this.objectMapper = objectMapper;
    this.webClient = webClientBuilder.build();
    this.queue = new ArrayBlockingQueue<>(Math.max(1, properties.publishQueueCapacity()));
  }

  @PostConstruct
  void start() {
    executor.submit(this::publishLoop);
  }

  public void publishMarketUpdated(
      String marketId,
      String eventId,
      String updateType,
      Instant receivedAt,
      Map<String, Object> source) {
    if (properties.graphqlUrl() == null || properties.graphqlUrl().isBlank()
        || properties.apiKey() == null || properties.apiKey().isBlank()
        || marketId == null || marketId.isBlank()) {
      return;
    }
    if (!properties.publishPriceUpdates() && ("Snapshot".equals(updateType) || "Incremental".equals(updateType))) {
      return;
    }

    try {
      String sourceJson = objectMapper.writeValueAsString(source);
      boolean accepted = queue.offer(new PublishRequest(marketId, eventId, updateType, receivedAt, sourceJson));
      if (!accepted) {
        int dropped = droppedSinceLastLog.incrementAndGet();
        if (dropped == 1 || dropped % 100 == 0) {
          log.warn("Dropped AppSync market update because publish queue is full dropped={}", dropped);
        }
      }
    } catch (JsonProcessingException e) {
      log.warn("Failed to serialize AppSync market update marketId={}", marketId, e);
    }
  }

  private void publishLoop() {
    while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
      try {
        publish(queue.take());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (RuntimeException e) {
        log.warn("Unexpected AppSync publish worker failure", e);
      }
    }
  }

  private void publish(PublishRequest request) {
    Map<String, Object> input = Map.of(
        "marketId", request.marketId(),
        "eventId", request.eventId() == null ? "" : request.eventId(),
        "updateType", request.updateType() == null ? "" : request.updateType(),
        "receivedAt", request.receivedAt().toString(),
        "source", request.sourceJson());
    Map<String, Object> body = Map.of(
        "query", MUTATION,
        "variables", Map.of("input", input));

    try {
      webClient.post()
          .uri(properties.graphqlUrl())
          .header(HttpHeaders.CONTENT_TYPE, "application/json")
          .header("x-api-key", properties.apiKey())
          .bodyValue(body)
          .retrieve()
          .bodyToMono(String.class)
          .block(properties.publishTimeout());
    } catch (RuntimeException e) {
      log.warn("Failed to publish AppSync market update marketId={}", request.marketId(), e);
    }
  }

  @PreDestroy
  void stop() throws InterruptedException {
    stopped.set(true);
    executor.shutdownNow();
    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
      log.warn("Timed out waiting for AppSync publish worker to stop");
    }
  }

  private record PublishRequest(
      String marketId,
      String eventId,
      String updateType,
      Instant receivedAt,
      String sourceJson) {
  }
}
