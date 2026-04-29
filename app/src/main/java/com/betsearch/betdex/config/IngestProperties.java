package com.betsearch.betdex.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ingest")
public record IngestProperties(
    int queueCapacity,
    int workerCount,
    Reconnect reconnect,
    Stream stream
) {
  public record Reconnect(Duration initialDelay, Duration maxDelay) {
  }

  public record Stream(Duration rotationInterval, Duration dedupeTtl) {
  }
}
