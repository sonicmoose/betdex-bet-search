package com.betsearch.betdex.betdex;

import com.betsearch.betdex.config.BetDexProperties;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Component
public class BetDexSessionClient {
  private static final Logger log = LoggerFactory.getLogger(BetDexSessionClient.class);

  private final BetDexProperties properties;
  private final WebClient webClient;
  private volatile CachedToken cachedToken;

  public BetDexSessionClient(BetDexProperties properties, WebClient.Builder webClientBuilder) {
    this.properties = properties;
    this.webClient = webClientBuilder.baseUrl(properties.restBaseUrl()).build();
  }

  public synchronized String accessToken() {
    if (cachedToken != null && cachedToken.expiresAt().isAfter(Instant.now().plusSeconds(60))) {
      return cachedToken.token();
    }

    // Schema: https://docs.api.monacoprotocol.xyz/v1/exchange-openapi.json#/components/schemas/CreateSessionRequest
    SessionResponse response = webClient.post()
        .uri(properties.sessionPath())
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(Map.of("appId", properties.appKey(), "apiKey", properties.apiKey()))
        .retrieve()
        .onStatus(HttpStatusCode::isError, clientResponse ->
            clientResponse.bodyToMono(String.class)
                .defaultIfEmpty("")
                .flatMap(body -> Mono.error(new IllegalStateException(
                    "BetDEX session request failed with HTTP " + clientResponse.statusCode().value()
                        + " from " + properties.restBaseUrl() + properties.sessionPath()
                        + ": " + body))))
        .bodyToMono(SessionResponse.class)
        .block();

    Session session = response == null ? null : response.firstSession();
    if (session == null || session.accessToken() == null || session.accessToken().isBlank()) {
      throw new IllegalStateException("BetDEX session response did not include an access token");
    }

    Instant expiresAt = session.accessExpiresAt() == null
        ? Instant.now().plusSeconds(20 * 60)
        : Instant.parse(session.accessExpiresAt());
    cachedToken = new CachedToken(session.accessToken(), expiresAt);
    log.info("Refreshed BetDEX stream access token expiring at {}", expiresAt);
    return cachedToken.token();
  }

  private record CachedToken(String token, Instant expiresAt) {
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record SessionResponse(List<Session> sessions) {
    Session firstSession() {
      return sessions == null || sessions.isEmpty() ? null : sessions.getFirst();
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record Session(String accessToken, String refreshToken, String accessExpiresAt, String refreshExpiresAt) {
  }
}
