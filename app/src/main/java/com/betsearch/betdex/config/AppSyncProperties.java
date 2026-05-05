package com.betsearch.betdex.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "appsync")
public record AppSyncProperties(
    String graphqlUrl,
    String apiKey,
    boolean publishPriceUpdates,
    int publishQueueCapacity,
    Duration publishTimeout) {
}
