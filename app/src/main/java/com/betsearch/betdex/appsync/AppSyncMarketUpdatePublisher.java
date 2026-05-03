package com.betsearch.betdex.appsync;

import com.betsearch.betdex.config.AppSyncProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
public class AppSyncMarketUpdatePublisher {
  private static final Logger log = LoggerFactory.getLogger(AppSyncMarketUpdatePublisher.class);
  private static final String MUTATION = """
      mutation PublishMarketUpdated($input: MarketUpdatedInput!) {
        publishMarketUpdated(input: $input) {
          marketId
          eventId
          updateType
          receivedAt
          source
        }
      }
      """;

  private final AppSyncProperties properties;
  private final ObjectMapper objectMapper;
  private final WebClient webClient;

  public AppSyncMarketUpdatePublisher(
      AppSyncProperties properties,
      ObjectMapper objectMapper,
      WebClient.Builder webClientBuilder) {
    this.properties = properties;
    this.objectMapper = objectMapper;
    this.webClient = webClientBuilder.build();
  }

  public void publishMarketUpdated(
      String marketId,
      String eventId,
      String updateType,
      Instant receivedAt,
      Map<String, Object> source) {
    if (properties.graphqlUrl() == null || properties.graphqlUrl().isBlank()
        || properties.apiKey() == null || properties.apiKey().isBlank()
        || marketId == null || marketId.isBlank()) {
      return;
    }

    try {
      Map<String, Object> input = Map.of(
          "marketId", marketId,
          "eventId", eventId == null ? "" : eventId,
          "updateType", updateType == null ? "" : updateType,
          "receivedAt", receivedAt.toString(),
          "source", objectMapper.writeValueAsString(source));
      Map<String, Object> body = Map.of(
          "query", MUTATION,
          "variables", Map.of("input", input));

      webClient.post()
          .uri(properties.graphqlUrl())
          .header(HttpHeaders.CONTENT_TYPE, "application/json")
          .header("x-api-key", properties.apiKey())
          .bodyValue(body)
          .retrieve()
          .bodyToMono(String.class)
          .doOnError(error -> log.warn("Failed to publish AppSync market update marketId={}", marketId, error))
          .subscribe();
    } catch (JsonProcessingException e) {
      log.warn("Failed to serialize AppSync market update marketId={}", marketId, e);
    }
  }
}
