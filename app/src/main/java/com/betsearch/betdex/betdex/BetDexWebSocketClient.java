package com.betsearch.betdex.betdex;

import com.betsearch.betdex.config.BetDexProperties;
import com.betsearch.betdex.config.IngestProperties;
import com.betsearch.betdex.ingest.InboundMessageQueue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
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
  private final Counter rotationCounter;
  private final Counter duplicateCounter;
  private final RecentMessageDeduper deduper;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final AtomicReference<Connection> activeConnection = new AtomicReference<>();
  private final AtomicReference<Connection> pendingReplacement = new AtomicReference<>();
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final AtomicLong connectionIds = new AtomicLong();
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
    this.rotationCounter = meterRegistry.counter("betdex.websocket.rotations");
    this.duplicateCounter = meterRegistry.counter("betdex.websocket.duplicates");
    this.deduper = new RecentMessageDeduper(ingestProperties.stream().dedupeTtl());
    this.reconnectDelay = ingestProperties.reconnect().initialDelay();
  }

  @Override
  public void run(ApplicationArguments args) {
    connect(false);
  }

  private void connect(boolean replacement) {
    if (stopped.get()) {
      return;
    }
    long connectionId = connectionIds.incrementAndGet();
    Connection connection = new Connection(connectionId, replacement);
    if (replacement && !pendingReplacement.compareAndSet(null, connection)) {
      log.info("BetDEX WebSocket replacement is already pending; skipping new rotation connection={}", connectionId);
      return;
    }
    log.info("Connecting to BetDEX stream {} connection={} replacement={}", properties.streamUrl(), connectionId, replacement);
    HttpClient.newHttpClient()
        .newWebSocketBuilder()
        .buildAsync(URI.create(properties.streamUrl()), connection)
        .whenComplete((socket, error) -> {
          if (error != null) {
            log.warn("BetDEX WebSocket connect failed connection={}", connectionId, error);
            pendingReplacement.compareAndSet(connection, null);
            scheduleReconnect(replacement && activeConnection.get() != null);
          }
        });
  }

  private void sendAuthenticationFrame(WebSocket socket) {
    send(socket, Map.of("action", "authenticate", "accessToken", sessionClient.accessToken()));
  }

  private void sendSubscriptionFrames(WebSocket socket) {
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
    scheduleReconnect(false);
  }

  private void scheduleReconnect(boolean replacement) {
    if (stopped.get()) {
      return;
    }
    reconnectCounter.increment();
    Duration delay = reconnectDelay;
    reconnectDelay = delay.multipliedBy(2).compareTo(ingestProperties.reconnect().maxDelay()) > 0
        ? ingestProperties.reconnect().maxDelay()
        : delay.multipliedBy(2);
    scheduler.schedule(() -> connect(replacement), delay.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void scheduleRotation(Connection connection) {
    Duration interval = ingestProperties.stream().rotationInterval();
    if (interval.isZero() || interval.isNegative()) {
      return;
    }

    ScheduledFuture<?> task = scheduler.schedule(() -> {
      if (!stopped.get() && activeConnection.get() == connection) {
        rotationCounter.increment();
        log.info("Rotating BetDEX WebSocket before connection age limit connection={}", connection.id);
        connect(true);
      }
    }, interval.toMillis(), TimeUnit.MILLISECONDS);
    connection.rotationTask.set(task);
  }

  private void promote(Connection connection, WebSocket socket) {
    connection.socket.set(socket);
    connection.promoted.set(true);
    pendingReplacement.compareAndSet(connection, null);
    reconnectDelay = ingestProperties.reconnect().initialDelay();
    Connection previous = activeConnection.getAndSet(connection);
    scheduleRotation(connection);
    if (previous != null && previous != connection) {
      previous.close("rotated");
    }
  }

  private void handleConnectionEnded(Connection connection, String source) {
    ScheduledFuture<?> task = connection.rotationTask.getAndSet(null);
    if (task != null) {
      task.cancel(false);
    }
    pendingReplacement.compareAndSet(connection, null);

    if (stopped.get() || connection.closedByClient.get()) {
      return;
    }

    if (activeConnection.compareAndSet(connection, null)) {
      log.warn("Active BetDEX WebSocket ended from {}; reconnecting connection={}", source, connection.id);
      scheduleReconnect(false);
    } else if (connection.replacement && activeConnection.get() != null) {
      log.warn("Replacement BetDEX WebSocket ended from {}; retrying rotation connection={}", source, connection.id);
      scheduleReconnect(true);
    } else {
      log.warn("Non-active BetDEX WebSocket ended from {} connection={}", source, connection.id);
    }
  }

  private void offerDeduped(String message) {
    if (!deduper.accept(fingerprint(message))) {
      duplicateCounter.increment();
      return;
    }

    if (!queue.offer(message)) {
      log.warn("Dropped BetDEX stream message because inbound queue is full");
    }
  }

  private String fingerprint(String message) {
    try {
      JsonNode node = objectMapper.readTree(message);
      return sha256(objectMapper.writeValueAsString(node));
    } catch (JsonProcessingException e) {
      return sha256(message);
    }
  }

  private String sha256(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 digest is not available", e);
    }
  }

  @PreDestroy
  void stop() {
    stopped.set(true);
    Connection connection = activeConnection.getAndSet(null);
    if (connection != null) {
      connection.close("shutdown");
    }
    Connection replacement = pendingReplacement.getAndSet(null);
    if (replacement != null) {
      replacement.close("shutdown");
    }
    scheduler.shutdownNow();
  }

  private final class Connection implements WebSocket.Listener {
    private final long id;
    private final boolean replacement;
    private final StringBuilder text = new StringBuilder();
    private final AtomicReference<WebSocket> socket = new AtomicReference<>();
    private final AtomicReference<ScheduledFuture<?>> rotationTask = new AtomicReference<>();
    private final AtomicBoolean closedByClient = new AtomicBoolean(false);
    private final AtomicBoolean ended = new AtomicBoolean(false);
    private final AtomicBoolean promoted = new AtomicBoolean(false);
    private final AtomicBoolean subscriptionsSent = new AtomicBoolean(false);

    private Connection(long id, boolean replacement) {
      this.id = id;
      this.replacement = replacement;
    }

    @Override
    public void onOpen(WebSocket webSocket) {
      socket.set(webSocket);
      WebSocket.Listener.super.onOpen(webSocket);
      sendAuthenticationFrame(webSocket);
      if (replacement) {
        log.info("BetDEX replacement WebSocket connected and authentication sent; waiting for stream data connection={}", id);
      } else {
        promote(this, webSocket);
        log.info("BetDEX WebSocket connected and authentication sent connection={}", id);
      }
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      text.append(data);
      if (last) {
        String message = text.toString();
        text.setLength(0);
        MessageKind kind = messageKind(message);
        if (kind == MessageKind.AUTHENTICATED && subscriptionsSent.compareAndSet(false, true)) {
          sendSubscriptionFrames(webSocket);
          log.info("BetDEX WebSocket authenticated and subscriptions sent connection={}", id);
        } else if (kind == MessageKind.SUBSCRIBE_NOT_AUTHENTICATED) {
          log.warn("BetDEX stream rejected a subscription before authentication; re-authenticating connection={}", id);
          subscriptionsSent.set(false);
          sendAuthenticationFrame(webSocket);
        }

        if (replacement && kind == MessageKind.DATA && promoted.compareAndSet(false, true)) {
          promote(this, webSocket);
          log.info("BetDEX replacement WebSocket received first data message and is now active connection={}", id);
        }
        offerDeduped(message);
      }
      webSocket.request(1);
      return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
      log.warn("BetDEX WebSocket closed connection={} status={} reason={}", id, statusCode, reason);
      if (ended.compareAndSet(false, true)) {
        handleConnectionEnded(this, "close");
      }
      return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      log.warn("BetDEX WebSocket error connection={}", id, error);
      if (ended.compareAndSet(false, true)) {
        handleConnectionEnded(this, "error");
      }
    }

    private void close(String reason) {
      closedByClient.set(true);
      ScheduledFuture<?> task = rotationTask.getAndSet(null);
      if (task != null) {
        task.cancel(false);
      }

      WebSocket current = socket.get();
      if (current != null) {
        current.sendClose(WebSocket.NORMAL_CLOSURE, reason);
      }
    }

    private MessageKind messageKind(String message) {
      try {
        JsonNode node = objectMapper.readTree(message);
        String type = text(node, "type");
        if ("AuthenticationUpdate".equals(type)) {
          return MessageKind.AUTHENTICATED;
        }
        if ("SubscribeNotAuthenticatedUpdate".equals(type)) {
          return MessageKind.SUBSCRIBE_NOT_AUTHENTICATED;
        }
        if ("SubscribeUpdate".equals(type)) {
          return MessageKind.SUBSCRIBE_ACK;
        }
        return MessageKind.DATA;
      } catch (JsonProcessingException e) {
        return MessageKind.DATA;
      }
    }

    private String text(JsonNode node, String field) {
      JsonNode value = node.get(field);
      return value == null || value.isNull() ? null : value.asText();
    }
  }

  private enum MessageKind {
    AUTHENTICATED,
    SUBSCRIBE_ACK,
    SUBSCRIBE_NOT_AUTHENTICATED,
    DATA
  }
}
