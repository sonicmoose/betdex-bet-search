package com.betsearch.betdex.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ingest")
public record IngestProperties(
    int queueCapacity,
    int workerCount,
    Reconnect reconnect
) {
  public record Reconnect(Duration initialDelay, Duration maxDelay) {
  }
}
