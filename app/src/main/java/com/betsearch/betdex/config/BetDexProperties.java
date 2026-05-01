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
    String marketsEventIdsParam,
    String marketsPageParam,
    String marketsPageSizeParam,
    int marketsBatchSize,
    int marketsFirstPage,
    int marketsPageSize,
    int marketsMaxPages,
    Duration marketCacheTtl
) {
}
