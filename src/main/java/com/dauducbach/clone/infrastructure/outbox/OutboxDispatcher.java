package com.dauducbach.clone.infrastructure.outbox;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "vector.outbox.enabled", havingValue = "true")
public class OutboxDispatcher {
    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);
    private final OutboxPublisher publisher;
    private final AtomicBoolean running = new AtomicBoolean();

    @Scheduled(fixedDelayString = "${vector.outbox.poll-delay-ms:1000}")
    public void dispatch() {
        if (!running.compareAndSet(false, true)) return;
        publisher.dispatchPending().doFinally(signal -> running.set(false))
                .subscribe(ignored -> {}, error -> log.warn("Outbox dispatch retained pending events", error));
    }
}
