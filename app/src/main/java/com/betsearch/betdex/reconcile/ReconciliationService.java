package com.betsearch.betdex.reconcile;

import com.betsearch.betdex.betdex.BetDexMarketEnrichmentService;
import com.betsearch.betdex.config.IngestProperties;
import com.betsearch.betdex.opensearch.OpenSearchWriter;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Service
public class ReconciliationService {
  private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

  private final IngestProperties properties;
  private final OpenSearchWriter openSearchWriter;
  private final BetDexMarketEnrichmentService enrichmentService;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  public ReconciliationService(
      IngestProperties properties,
      OpenSearchWriter openSearchWriter,
      BetDexMarketEnrichmentService enrichmentService) {
    this.properties = properties;
    this.openSearchWriter = openSearchWriter;
    this.enrichmentService = enrichmentService;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void startReconciliation() {
    if (!properties.reconciliation().enabled()) {
      log.info("BetDEX reconciliation is disabled");
      return;
    }
    long initialDelay = Math.max(0, properties.reconciliation().initialDelay().toMillis());
    long interval = Math.max(1, properties.reconciliation().interval().toMillis());
    executor.scheduleWithFixedDelay(this::reconcileSafely, initialDelay, interval, TimeUnit.MILLISECONDS);
    log.info(
        "BetDEX reconciliation scheduled initialDelay={} interval={} maxMarkets={} batchSize={}",
        properties.reconciliation().initialDelay(),
        properties.reconciliation().interval(),
        properties.reconciliation().maxMarkets(),
        properties.reconciliation().batchSize());
  }

  public void reconcile() {
    int maxMarkets = Math.max(1, properties.reconciliation().maxMarkets());
    int batchSize = Math.max(1, properties.reconciliation().batchSize());
    List<String> marketIds = openSearchWriter.openMarketIds(maxMarkets);
    if (marketIds.isEmpty()) {
      log.info("BetDEX reconciliation found no open markets to refresh");
      return;
    }
    log.info("BetDEX reconciliation selected {} open markets to refresh", marketIds.size());

    int refreshed = 0;
    for (List<String> batch : batches(marketIds, batchSize)) {
      refreshed += enrichmentService.refreshMarketsByMarketIds(batch);
    }
    log.info("BetDEX reconciliation refreshed {} open markets requested={}", refreshed, marketIds.size());
  }

  private void reconcileSafely() {
    try {
      reconcile();
    } catch (RuntimeException e) {
      log.warn("BetDEX reconciliation failed", e);
    }
  }

  private List<List<String>> batches(List<String> values, int batchSize) {
    List<List<String>> result = new ArrayList<>();
    for (int index = 0; index < values.size(); index += batchSize) {
      result.add(values.subList(index, Math.min(values.size(), index + batchSize)));
    }
    return result;
  }

  @PreDestroy
  void stop() {
    executor.shutdownNow();
  }
}
