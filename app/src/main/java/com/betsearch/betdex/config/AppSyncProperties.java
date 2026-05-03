package com.betsearch.betdex.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "appsync")
public record AppSyncProperties(
    String graphqlUrl,
    String apiKey) {
}
