package com.betsearch.betdex.betdex;

import com.betsearch.betdex.config.BetDexProperties;
import com.betsearch.betdex.config.IngestProperties;
import com.betsearch.betdex.ingest.InboundMessageQueue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class BetDexWebSocketClient implements ApplicationRunner {
  private static final Logger log = LoggerFactory.getLogger(BetDexWebSocketClient.class);
  private static final List<String> SUBSCRIPTION_TYPES = List.of(
      "EventUpdate",
      "MarketUpdate",
      "MarketPriceUpdate",
      "MarketStatusUpdate");

  private final BetDexProperties properties;
  private final IngestProperties ingestProperties;
  private final BetDexSessionClient sessionClient;
  private final InboundMessageQueue queue;
  private final ObjectMapper objectMapper;
  private final Counter reconnectCounter;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final AtomicReference<WebSocket> webSocket = new AtomicReference<>();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private volatile Duration reconnectDelay;

  public BetDexWebSocketClient(
      BetDexProperties properties,
      IngestProperties ingestProperties,
      BetDexSessionClient sessionClient,
      InboundMessageQueue queue,
      ObjectMapper objectMapper,
      MeterRegistry meterRegistry) {
    this.properties = properties;
    this.ingestProperties = ingestProperties;
    this.sessionClient = sessionClient;
    this.queue = queue;
    this.objectMapper = objectMapper;
    this.reconnectCounter = meterRegistry.counter("betdex.websocket.reconnects");
    this.reconnectDelay = ingestProperties.reconnect().initialDelay();
  }

  @Override
  public void run(ApplicationArguments args) {
    connect();
  }

  private void connect() {
    if (stopped.get()) {
      return;
    }
    log.info("Connecting to BetDEX stream {}", properties.streamUrl());
    HttpClient.newHttpClient()
        .newWebSocketBuilder()
        .buildAsync(URI.create(properties.streamUrl()), new Listener())
        .whenComplete((socket, error) -> {
          if (error != null) {
            log.warn("BetDEX WebSocket connect failed", error);
            scheduleReconnect();
          } else {
            webSocket.set(socket);
          }
        });
  }

  private void sendStartupFrames(WebSocket socket) {
    send(socket, Map.of("action", "authenticate", "accessToken", sessionClient.accessToken()));
    SUBSCRIPTION_TYPES.forEach(type -> send(socket, Map.of(
        "action", "subscribe",
        "subscriptionType", type,
        "subscriptionIds", List.of("*"))));
  }

  private void send(WebSocket socket, Map<String, ?> frame) {
    try {
      socket.sendText(objectMapper.writeValueAsString(frame), true);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize WebSocket frame", e);
    }
  }

  private void scheduleReconnect() {
    if (stopped.get()) {
      return;
    }
    reconnectCounter.increment();
    Duration delay = reconnectDelay;
    reconnectDelay = delay.multipliedBy(2).compareTo(ingestProperties.reconnect().maxDelay()) > 0
        ? ingestProperties.reconnect().maxDelay()
        : delay.multipliedBy(2);
    scheduler.schedule(this::connect, delay.toMillis(), TimeUnit.MILLISECONDS);
  }

  @PreDestroy
  void stop() {
    stopped.set(true);
    WebSocket socket = webSocket.get();
    if (socket != null) {
      socket.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown");
    }
    scheduler.shutdownNow();
  }

  private final class Listener implements WebSocket.Listener {
    private final StringBuilder text = new StringBuilder();

    @Override
    public void onOpen(WebSocket webSocket) {
      reconnectDelay = ingestProperties.reconnect().initialDelay();
      WebSocket.Listener.super.onOpen(webSocket);
      sendStartupFrames(webSocket);
      log.info("BetDEX WebSocket connected and subscriptions sent");
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      text.append(data);
      if (last) {
        String message = text.toString();
        text.setLength(0);
        if (!queue.offer(message)) {
          log.warn("Dropped BetDEX stream message because inbound queue is full");
        }
      }
      webSocket.request(1);
      return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
      log.warn("BetDEX WebSocket closed status={} reason={}", statusCode, reason);
      scheduleReconnect();
      return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      log.warn("BetDEX WebSocket error", error);
      scheduleReconnect();
    }
  }
}
