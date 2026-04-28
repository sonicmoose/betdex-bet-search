package com.betsearch.betdex.timestream;

import com.betsearch.betdex.ingest.PriceUpdate;
import java.util.List;

public interface TimestreamWriter {
  void writePrices(List<PriceUpdate> prices);
}
