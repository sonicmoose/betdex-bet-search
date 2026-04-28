package com.betsearch.betdex.ingest;

import com.betsearch.betdex.config.IngestProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.springframework.stereotype.Component;

@Component
public class InboundMessageQueue {
  private final BlockingQueue<String> queue;
  private final Counter receivedCounter;
  private final Counter droppedCounter;

  public InboundMessageQueue(IngestProperties properties, MeterRegistry meterRegistry) {
    this.queue = new ArrayBlockingQueue<>(properties.queueCapacity());
    this.receivedCounter = meterRegistry.counter("betdex.messages.received");
    this.droppedCounter = meterRegistry.counter("betdex.messages.dropped");
    meterRegistry.gauge("betdex.queue.depth", queue, BlockingQueue::size);
  }

  public boolean offer(String message) {
    receivedCounter.increment();
    boolean accepted = queue.offer(message);
    if (!accepted) {
      droppedCounter.increment();
    }
    return accepted;
  }

  public String take() throws InterruptedException {
    return queue.take();
  }
}
