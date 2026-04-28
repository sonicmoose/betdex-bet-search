package com.betsearch.betdex.ingest;

import com.betsearch.betdex.config.IngestProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class IngestWorkerService {
  private static final Logger log = LoggerFactory.getLogger(IngestWorkerService.class);

  private final InboundMessageQueue queue;
  private final MarketMessageHandler handler;
  private final ExecutorService executorService;
  private final List<Runnable> workers = new ArrayList<>();

  public IngestWorkerService(
      InboundMessageQueue queue,
      MarketMessageHandler handler,
      IngestProperties properties) {
    this.queue = queue;
    this.handler = handler;
    this.executorService = Executors.newFixedThreadPool(properties.workerCount());
    for (int i = 0; i < properties.workerCount(); i++) {
      workers.add(this::runWorker);
    }
  }

  @PostConstruct
  void start() {
    workers.forEach(executorService::submit);
  }

  private void runWorker() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        handler.handle(queue.take());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (RuntimeException e) {
        log.warn("Unexpected ingest worker failure", e);
      }
    }
  }

  @PreDestroy
  void stop() throws InterruptedException {
    executorService.shutdownNow();
    if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
      log.warn("Timed out waiting for ingest workers to stop");
    }
  }
}
