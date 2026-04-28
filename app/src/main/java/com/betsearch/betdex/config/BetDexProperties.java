package com.betsearch.betdex.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "betdex")
public record BetDexProperties(
    String restBaseUrl,
    String streamUrl,
    String appKey,
    String apiKey,
    String sessionPath
) {
}
