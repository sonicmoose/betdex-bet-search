package com.betsearch.betdex.ingest;

import com.betsearch.betdex.config.IngestProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class IngestWorkerService {
  private static final Logger log = LoggerFactory.getLogger(IngestWorkerService.class);

  private final InboundMessageQueue queue;
  private final MarketMessageHandler handler;
  private final ObjectMapper objectMapper;
  private final ExecutorService dispatcher;
  private final List<ThreadPoolExecutor> partitions = new ArrayList<>();

  public IngestWorkerService(
      InboundMessageQueue queue,
      MarketMessageHandler handler,
      ObjectMapper objectMapper,
      IngestProperties properties,
      MeterRegistry meterRegistry) {
    this.queue = queue;
    this.handler = handler;
    this.objectMapper = objectMapper;
    this.dispatcher = Executors.newSingleThreadExecutor();
    int partitionCount = Math.max(1, properties.workerCount());
    for (int i = 0; i < partitionCount; i++) {
      BlockingQueue<Runnable> partitionQueue = new LinkedBlockingQueue<>();
      ThreadPoolExecutor partition = new ThreadPoolExecutor(
          1,
          1,
          0L,
          TimeUnit.MILLISECONDS,
          partitionQueue);
      partitions.add(partition);
      meterRegistry.gauge(
          "betdex.ingest.partition.depth",
          Tags.of("partition", Integer.toString(i)),
          partitionQueue,
          BlockingQueue::size);
    }
  }

  @PostConstruct
  void start() {
    dispatcher.submit(this::runDispatcher);
  }

  private void runDispatcher() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        String rawJson = queue.take();
        partitions.get(partition(rawJson)).submit(() -> handle(rawJson));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (RejectedExecutionException e) {
        if (!Thread.currentThread().isInterrupted()) {
          log.warn("Ingest partition rejected message", e);
        }
      } catch (RuntimeException e) {
        log.warn("Unexpected ingest dispatcher failure", e);
      }
    }
  }

  private void handle(String rawJson) {
    try {
      handler.handle(rawJson);
    } catch (RuntimeException e) {
      log.warn("Unexpected ingest worker failure", e);
    }
  }

  private int partition(String rawJson) {
    String key = messageKey(rawJson);
    return Math.floorMod(key.hashCode(), partitions.size());
  }

  private String messageKey(String rawJson) {
    try {
      JsonNode root = objectMapper.readTree(rawJson);
      JsonNode message = messageNode(root);
      String marketId = firstText(root, message, "marketId", "id");
      if (marketId != null) {
        return "market:" + marketId;
      }
      String eventId = firstText(root, message, "eventId");
      if (eventId != null) {
        return "event:" + eventId;
      }
    } catch (JsonProcessingException e) {
      return rawJson;
    }
    return rawJson;
  }

  private JsonNode messageNode(JsonNode root) {
    for (String field : List.of("data", "payload", "message")) {
      JsonNode candidate = root.get(field);
      if (candidate != null && candidate.isObject()) {
        return candidate;
      }
    }
    return root;
  }

  private String firstText(JsonNode root, JsonNode message, String... fields) {
    for (String field : fields) {
      String value = text(root, field);
      if (value != null) {
        return value;
      }
      value = text(message, field);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private String text(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() || value.asText().isBlank() ? null : value.asText();
  }

  @PreDestroy
  void stop() throws InterruptedException {
    dispatcher.shutdownNow();
    if (!dispatcher.awaitTermination(10, TimeUnit.SECONDS)) {
      log.warn("Timed out waiting for ingest dispatcher to stop");
    }
    for (ExecutorService partition : partitions) {
      partition.shutdownNow();
    }
    for (ExecutorService partition : partitions) {
      if (!partition.awaitTermination(10, TimeUnit.SECONDS)) {
        log.warn("Timed out waiting for ingest partition worker to stop");
      }
    }
  }
}
