package com.betsearch.betdex.timestream;

import com.betsearch.betdex.config.TimestreamProperties;
import com.betsearch.betdex.ingest.PriceUpdate;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.RejectedRecordsException;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

@Component
@ConditionalOnProperty(prefix = "timestream", name = "enabled", havingValue = "true")
public class AwsTimestreamWriter implements TimestreamWriter {
  private static final Logger log = LoggerFactory.getLogger(AwsTimestreamWriter.class);

  private final TimestreamWriteClient client;
  private final TimestreamProperties properties;

  public AwsTimestreamWriter(TimestreamWriteClient client, TimestreamProperties properties) {
    this.client = client;
    this.properties = properties;
  }

  @Override
  public void writePrices(List<PriceUpdate> prices) {
    if (prices.isEmpty()) {
      return;
    }
    List<Record> records = prices.stream().map(this::toRecord).toList();
    for (int from = 0; from < records.size(); from += 100) {
      List<Record> batch = records.subList(from, Math.min(from + 100, records.size()));
      writeWithRetry(batch);
    }
  }

  private Record toRecord(PriceUpdate price) {
    Instant time = price.validAt() == null ? price.receivedAt() : price.validAt();
    return Record.builder()
        .dimensions(dimensions(price))
        .measureName("market_price")
        .measureValueType(MeasureValueType.MULTI)
        .measureValues(measures(price))
        .time(Long.toString(time.toEpochMilli()))
        .timeUnit(TimeUnit.MILLISECONDS)
        .build();
  }

  private List<Dimension> dimensions(PriceUpdate price) {
    return List.of(
        dimension("marketId", price.marketId()),
        dimension("eventId", price.eventId()),
        dimension("eventGroupId", price.eventGroupId()),
        dimension("categoryId", price.categoryId()),
        dimension("subCategoryId", price.subCategoryId()),
        dimension("outcomeId", price.outcomeId()),
        dimension("side", price.side()),
        dimension("currencyId", price.currencyId()));
  }

  private Dimension dimension(String name, String value) {
    return Dimension.builder().name(name).value(value == null ? "unknown" : value).build();
  }

  private List<MeasureValue> measures(PriceUpdate price) {
    List<MeasureValue> values = new ArrayList<>();
    addMeasure(values, "price", price.price());
    addMeasure(values, "liquidity", price.liquidity());
    addMeasure(values, "change", price.change());
    return values;
  }

  private void addMeasure(List<MeasureValue> values, String name, BigDecimal value) {
    if (value != null) {
      values.add(MeasureValue.builder()
          .name(name)
          .value(value.toPlainString())
          .type(MeasureValueType.DOUBLE)
          .build());
    }
  }

  private void writeWithRetry(List<Record> records) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= 3; attempt++) {
      try {
        client.writeRecords(WriteRecordsRequest.builder()
            .databaseName(properties.database())
            .tableName(properties.table())
            .records(records)
            .build());
        return;
      } catch (RejectedRecordsException e) {
        throw e;
      } catch (RuntimeException e) {
        last = e;
        sleepBackoff(attempt);
      }
    }
    throw last;
  }

  private void sleepBackoff(int attempt) {
    try {
      Thread.sleep(250L * attempt * attempt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.warn("Interrupted during Timestream retry backoff", e);
    }
  }
}
