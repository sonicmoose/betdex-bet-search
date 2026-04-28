package com.betsearch.betdex.timestream;

import com.betsearch.betdex.ingest.PriceUpdate;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "timestream", name = "enabled", havingValue = "false", matchIfMissing = true)
public class NoopTimestreamWriter implements TimestreamWriter {
  @Override
  public void writePrices(List<PriceUpdate> prices) {
  }
}
