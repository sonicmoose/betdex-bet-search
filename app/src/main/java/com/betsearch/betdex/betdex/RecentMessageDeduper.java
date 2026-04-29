package com.betsearch.betdex.betdex;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class RecentMessageDeduper {
  private final Clock clock;
  private final long ttlMillis;
  private final Map<String, Long> expiresAtByFingerprint = new HashMap<>();
  private long nextCleanupAt;

  RecentMessageDeduper(Duration ttl) {
    this(ttl, Clock.systemUTC());
  }

  RecentMessageDeduper(Duration ttl, Clock clock) {
    this.clock = clock;
    this.ttlMillis = Math.max(0, ttl.toMillis());
  }

  synchronized boolean accept(String fingerprint) {
    if (ttlMillis == 0) {
      return true;
    }

    long now = clock.millis();
    if (now >= nextCleanupAt) {
      cleanup(now);
      nextCleanupAt = now + ttlMillis;
    }

    Long expiresAt = expiresAtByFingerprint.get(fingerprint);
    if (expiresAt != null && expiresAt > now) {
      return false;
    }

    expiresAtByFingerprint.put(fingerprint, now + ttlMillis);
    return true;
  }

  private void cleanup(long now) {
    Iterator<Map.Entry<String, Long>> iterator = expiresAtByFingerprint.entrySet().iterator();
    while (iterator.hasNext()) {
      if (iterator.next().getValue() <= now) {
        iterator.remove();
      }
    }
  }
}
