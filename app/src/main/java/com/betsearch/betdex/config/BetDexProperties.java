package com.betsearch.betdex.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "betdex")
public record BetDexProperties(
    String restBaseUrl,
    String streamUrl,
    String appKey,
    String apiKey,
    String sessionPath,
    String marketsPath,
    String marketsIdsParam,
    int marketsBatchSize,
    Duration marketCacheTtl
) {
}
