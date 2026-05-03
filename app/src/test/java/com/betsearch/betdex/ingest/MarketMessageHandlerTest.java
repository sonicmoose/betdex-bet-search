package com.betsearch.betdex.ingest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.betsearch.betdex.appsync.AppSyncMarketUpdatePublisher;
import com.betsearch.betdex.betdex.BetDexMarketEnrichmentService;
import com.betsearch.betdex.config.IngestProperties;
import com.betsearch.betdex.opensearch.OpenSearchWriter;
import com.betsearch.betdex.timestream.TimestreamWriter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class MarketMessageHandlerTest {
  private final OpenSearchWriter openSearchWriter = mock(OpenSearchWriter.class);
  private final TimestreamWriter timestreamWriter = mock(TimestreamWriter.class);
  private final BetDexMarketEnrichmentService marketEnrichmentService = mock(BetDexMarketEnrichmentService.class);
  private final AppSyncMarketUpdatePublisher marketUpdatePublisher = mock(AppSyncMarketUpdatePublisher.class);
  private final IngestProperties ingestProperties = new IngestProperties(
      1000,
      1,
      true,
      true,
      false,
      new IngestProperties.Reconnect(Duration.ofSeconds(1), Duration.ofMinutes(1)),
      new IngestProperties.Stream(Duration.ofMinutes(115), Duration.ofMinutes(10)),
      new IngestProperties.Reconciliation(true, Duration.ofMinutes(2), Duration.ofMinutes(10), 1000, 50));
  private final MarketMessageHandler handler = new MarketMessageHandler(
      new ObjectMapper(),
      openSearchWriter,
      timestreamWriter,
      marketEnrichmentService,
      marketUpdatePublisher,
      ingestProperties,
      new SimpleMeterRegistry());

  @Test
  void routesMarketUpdateToCurrentMarketIndex() {
    when(openSearchWriter.upsertMarket(eq("m-1"), any(), any()))
        .thenReturn(Map.of("marketId", "m-1", "status", "Open"));

    handler.handle("""
        {
          "marketId": "m-1",
          "eventId": "e-1",
          "marketOutcomes": [],
          "name": "Winner",
          "status": "Open"
        }
        """);

    verify(openSearchWriter).indexRaw(any(), eq("MarketUpdate"), eq("m-1"), eq("e-1"), any());
    verify(openSearchWriter).upsertMarket(eq("m-1"), any(), any());
    verify(marketUpdatePublisher).publishMarketUpdated(eq("m-1"), eq("e-1"), eq("MarketUpdate"), any(), any());
  }

  @Test
  void publishesMarketStatusUpdatesSoVisibleRowsCanBeRemoved() {
    when(openSearchWriter.upsertMarketStatus(eq("m-1"), any(), any()))
        .thenReturn(Map.of("marketId", "m-1", "status", "Settled"));

    handler.handle("""
        {
          "type": "MarketStatusUpdate",
          "marketId": "m-1",
          "eventId": "e-1",
          "status": "Settled"
        }
        """);

    verify(openSearchWriter).indexRaw(any(), eq("MarketStatusUpdate"), eq("m-1"), eq("e-1"), any());
    verify(openSearchWriter).upsertMarketStatus(eq("m-1"), any(), any());
    verify(marketUpdatePublisher).publishMarketUpdated(eq("m-1"), eq("e-1"), eq("MarketStatusUpdate"), any(), any());
  }

  @Test
  void doesNotTreatSubscriptionAcknowledgementAsMarketStatusUpdate() {
    handler.handle("""
        {
          "type": "SubscribeUpdate",
          "subscriptionType": "MarketStatusUpdate",
          "subscriptionId": "*"
        }
        """);

    verify(openSearchWriter).indexRaw(any(), eq("SubscribeUpdate"), any(), any(), any());
    verify(openSearchWriter, never()).upsertMarketStatus(any(), any(), any());
    verify(marketUpdatePublisher, never()).publishMarketUpdated(any(), any(), any(), any(), any());
  }

  @Test
  void flattensMarketPriceUpdateForOpenSearchAndTimestream() {
	    when(marketEnrichmentService.cachedEnrichmentForPrices(any())).thenReturn(Map.of());
	    when(openSearchWriter.upsertMarketPrices(any(), eq(Map.of()))).thenReturn(Map.of("marketId", "m-1"));
	    handler.handle("""
	        {
	          "updateType": "Snapshot",
	          "marketId": "m-1",
	          "eventId": "e-1",
	          "eventGroupId": "eg-1",
	          "categoryId": "cat-1",
	          "subCategoryId": "sub-1",
	          "currencyId": "USDC",
          "prices": [
            {
              "side": "For",
              "outcomeId": "o-1",
              "price": 2.14,
              "liquidity": 123.45,
              "change": 10.0,
              "validAt": "2026-04-28T12:00:00Z"
            }
          ]
        }
        """);

	    ArgumentCaptor<PriceUpdate> priceCaptor = ArgumentCaptor.forClass(PriceUpdate.class);
	    verify(openSearchWriter).indexPrice(priceCaptor.capture(), eq(Map.of()));
	    PriceUpdate price = priceCaptor.getValue();
	    org.assertj.core.api.Assertions.assertThat(price.marketId()).isEqualTo("m-1");
	    org.assertj.core.api.Assertions.assertThat(price.eventId()).isEqualTo("e-1");
	    org.assertj.core.api.Assertions.assertThat(price.eventGroupId()).isEqualTo("eg-1");
	    org.assertj.core.api.Assertions.assertThat(price.categoryId()).isEqualTo("cat-1");
	    org.assertj.core.api.Assertions.assertThat(price.subCategoryId()).isEqualTo("sub-1");
	    org.assertj.core.api.Assertions.assertThat(price.outcomeId()).isEqualTo("o-1");
	    org.assertj.core.api.Assertions.assertThat(price.price()).isEqualByComparingTo(new BigDecimal("2.14"));
	    org.assertj.core.api.Assertions.assertThat(price.source())
	        .containsEntry("eventGroupId", "eg-1")
	        .containsEntry("categoryId", "cat-1")
	        .containsEntry("subCategoryId", "sub-1");

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PriceUpdate>> listCaptor = ArgumentCaptor.forClass(List.class);
    verify(openSearchWriter).upsertMarketPrices(listCaptor.capture(), eq(Map.of()));
    org.assertj.core.api.Assertions.assertThat(listCaptor.getValue()).hasSize(1);
    verify(marketUpdatePublisher).publishMarketUpdated(eq("m-1"), eq("e-1"), eq("Snapshot"), any(), any());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PriceUpdate>> timestreamCaptor = ArgumentCaptor.forClass(List.class);
    verify(timestreamWriter).writePrices(timestreamCaptor.capture());
    org.assertj.core.api.Assertions.assertThat(timestreamCaptor.getValue()).hasSize(1);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<PriceUpdate>> enrichmentCaptor = ArgumentCaptor.forClass(List.class);
    verify(marketEnrichmentService).requestMarketEnrichment(enrichmentCaptor.capture());
    org.assertj.core.api.Assertions.assertThat(enrichmentCaptor.getValue()).hasSize(1);
  }
}
