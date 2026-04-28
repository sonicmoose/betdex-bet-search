package com.betsearch.betdex.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "timestream")
public record TimestreamProperties(
    boolean enabled,
    String database,
    String table
) {
}
