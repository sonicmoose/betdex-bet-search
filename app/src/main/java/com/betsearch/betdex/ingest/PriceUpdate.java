package com.betsearch.betdex.ingest;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

public record PriceUpdate(
    String marketId,
    String eventId,
    String eventGroupId,
    String categoryId,
    String subCategoryId,
    String outcomeId,
    String side,
    String currencyId,
    BigDecimal price,
    BigDecimal liquidity,
    BigDecimal change,
    Instant validAt,
    Instant receivedAt,
    String updateType,
    Map<String, Object> source
) {
}
