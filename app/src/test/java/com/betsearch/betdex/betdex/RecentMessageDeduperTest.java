package com.betsearch.betdex.betdex;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RecentMessageDeduperTest {
  @Test
  void rejectsDuplicateFingerprintsWithinTtl() {
    RecentMessageDeduper deduper = new RecentMessageDeduper(Duration.ofMinutes(10));

    assertThat(deduper.accept("message-1")).isTrue();
    assertThat(deduper.accept("message-1")).isFalse();
    assertThat(deduper.accept("message-2")).isTrue();
  }

  @Test
  void acceptsEverythingWhenTtlIsDisabled() {
    RecentMessageDeduper deduper = new RecentMessageDeduper(Duration.ZERO);

    assertThat(deduper.accept("message-1")).isTrue();
    assertThat(deduper.accept("message-1")).isTrue();
  }
}
