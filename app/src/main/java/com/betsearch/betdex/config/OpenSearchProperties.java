package com.betsearch.betdex.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "opensearch")
public record OpenSearchProperties(
    String endpoint,
    String rawAlias,
    String pricesAlias,
    String marketsCurrentIndex,
    String eventsCurrentIndex
) {
}
