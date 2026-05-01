package com.betsearch.betdex.ingest;

import com.betsearch.betdex.betdex.BetDexMarketEnrichmentService;
import com.betsearch.betdex.opensearch.OpenSearchWriter;
import com.betsearch.betdex.timestream.TimestreamWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class MarketMessageHandler {
  private static final Logger log = LoggerFactory.getLogger(MarketMessageHandler.class);
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
  };

  private final ObjectMapper objectMapper;
  private final OpenSearchWriter openSearchWriter;
  private final TimestreamWriter timestreamWriter;
  private final BetDexMarketEnrichmentService marketEnrichmentService;
  private final Counter indexedCounter;
  private final Counter failureCounter;

  public MarketMessageHandler(
      ObjectMapper objectMapper,
      OpenSearchWriter openSearchWriter,
      TimestreamWriter timestreamWriter,
      BetDexMarketEnrichmentService marketEnrichmentService,
      MeterRegistry meterRegistry) {
    this.objectMapper = objectMapper;
    this.openSearchWriter = openSearchWriter;
    this.timestreamWriter = timestreamWriter;
    this.marketEnrichmentService = marketEnrichmentService;
    this.indexedCounter = meterRegistry.counter("betdex.messages.indexed");
    this.failureCounter = meterRegistry.counter("betdex.messages.write_failures");
  }

  public void handle(String rawJson) {
    Instant receivedAt = Instant.now();
    try {
      JsonNode root = objectMapper.readTree(rawJson);
      JsonNode message = messageNode(root);
      Map<String, Object> payload = objectMapper.convertValue(root, MAP_TYPE);
      String messageType = detectMessageType(root, message);
      String marketId = firstText(root, message, "marketId", "id");
      String eventId = firstText(root, message, "eventId");

      log.info("Message received: {}", rawJson);

      openSearchWriter.indexRaw(receivedAt, messageType, marketId, eventId, payload);
      routeProjection(message, payload, messageType, receivedAt);
      indexedCounter.increment();
    } catch (JsonProcessingException e) {
      log.warn("Malformed BetDEX stream message; indexing raw text only", e);
      indexMalformed(receivedAt, rawJson);
    } catch (RuntimeException e) {
      failureCounter.increment();
      log.warn("Failed to index BetDEX stream message", e);
    }
  }

  private void indexMalformed(Instant receivedAt, String rawJson) {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("raw", rawJson);
    try {
      openSearchWriter.indexRaw(receivedAt, "Malformed", null, null, payload);
    } catch (RuntimeException e) {
      failureCounter.increment();
      log.warn("Failed to index malformed raw BetDEX message", e);
    }
  }

  private void routeProjection(JsonNode root, Map<String, Object> payload, String messageType, Instant receivedAt) {
    switch (messageType) {
      case "EventUpdate" -> openSearchWriter.upsertEvent(text(root, "eventId"), receivedAt, payload);
      case "MarketUpdate" -> openSearchWriter.upsertMarket(text(root, "marketId"), receivedAt, payload);
      case "MarketStatusUpdate" -> openSearchWriter.upsertMarketStatus(text(root, "marketId"), receivedAt, payload);
      case "MarketPriceUpdate" -> {
        List<PriceUpdate> prices = flattenPrices(root, receivedAt);
        for (PriceUpdate price : prices) {
          openSearchWriter.indexPrice(price);
        }
        openSearchWriter.upsertMarketPrices(prices);
        marketEnrichmentService.requestMarketEnrichment(prices);
        timestreamWriter.writePrices(prices);
      }
      default -> {
      }
    }
  }

  private List<PriceUpdate> flattenPrices(JsonNode root, Instant receivedAt) {
    List<PriceUpdate> result = new ArrayList<>();
    JsonNode prices = root.path("prices");
    if (!prices.isArray()) {
      return result;
    }
    for (JsonNode priceNode : prices) {
      Instant validAt = instant(priceNode, "validAt");
      Map<String, Object> source = new LinkedHashMap<>();
      source.put("marketId", text(root, "marketId"));
      source.put("eventId", text(root, "eventId"));
      source.put("eventGroupId", text(root, "eventGroupId"));
      source.put("categoryId", text(root, "categoryId"));
      source.put("subCategoryId", text(root, "subCategoryId"));
      source.put("outcomeId", text(priceNode, "outcomeId"));
      source.put("side", text(priceNode, "side"));
      source.put("currencyId", text(root, "currencyId"));
      source.put("price", decimal(priceNode, "price"));
      source.put("liquidity", decimal(priceNode, "liquidity"));
      source.put("change", decimal(priceNode, "change"));
      source.put("validAt", (validAt == null ? receivedAt : validAt).toString());
      source.put("receivedAt", receivedAt.toString());
      source.put("updateType", text(root, "updateType"));
      result.add(new PriceUpdate(
          text(root, "marketId"),
          text(root, "eventId"),
          text(root, "eventGroupId"),
          text(root, "categoryId"),
          text(root, "subCategoryId"),
          text(priceNode, "outcomeId"),
          text(priceNode, "side"),
          text(root, "currencyId"),
          decimal(priceNode, "price"),
          decimal(priceNode, "liquidity"),
          decimal(priceNode, "change"),
          validAt,
          receivedAt,
          text(root, "updateType"),
          source));
    }
    return result;
  }

  private JsonNode messageNode(JsonNode root) {
    for (String field : List.of("data", "payload", "message")) {
      JsonNode candidate = root.get(field);
      if (candidate != null && candidate.isObject()) {
        return candidate;
      }
    }
    return root;
  }

  private String detectMessageType(JsonNode root, JsonNode message) {
    String explicit = firstText(root, message, "messageType", "subscriptionType", "type");
    if (explicit != null && explicit.endsWith("Update")) {
      return explicit;
    }
    if (message.has("prices")) {
      return "MarketPriceUpdate";
    }
    if (message.has("marketOutcomes")) {
      return "MarketUpdate";
    }
    if (message.has("status") && (message.has("marketId") || message.has("id"))) {
      return "MarketStatusUpdate";
    }
    if ((message.has("eventId") || message.has("id")) && message.has("expectedStartTime")) {
      return "EventUpdate";
    }
    return explicit == null ? "Unknown" : explicit;
  }

  private String firstText(JsonNode root, JsonNode message, String... fields) {
    for (String field : fields) {
      String value = text(root, field);
      if (value != null) {
        return value;
      }
      value = text(message, field);
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private String text(JsonNode node, String field) {
    JsonNode value = node.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }

  private BigDecimal decimal(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull() || value.asText().isBlank()) {
      return null;
    }
    return value.decimalValue();
  }

  private Instant instant(JsonNode node, String field) {
    String value = text(node, field);
    return value == null ? null : Instant.parse(value);
  }
}
