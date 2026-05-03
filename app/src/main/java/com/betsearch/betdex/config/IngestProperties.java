package com.betsearch.betdex.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ingest")
public record IngestProperties(
    int queueCapacity,
    int workerCount,
    boolean rawIndexEnabled,
    boolean priceHistoryEnabled,
    boolean messageLoggingEnabled,
    Reconnect reconnect,
    Stream stream,
    Reconciliation reconciliation
) {
  public record Reconnect(Duration initialDelay, Duration maxDelay) {
  }

  public record Stream(Duration rotationInterval, Duration dedupeTtl) {
  }

  public record Reconciliation(
      boolean enabled,
      Duration initialDelay,
      Duration interval,
      int maxMarkets,
      int batchSize) {
  }
}
