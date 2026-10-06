package com.dauducbach.clone.infrastructure.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import java.time.Duration;
import java.util.UUID;

@Service
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxRepository repository;
    private final KafkaSender<String, String> sender;
    private final int batchSize;
    private final Duration sendTimeout;
    private final long leaseSeconds;

    public OutboxPublisher(OutboxRepository repository, KafkaSender<String, String> sender,
            @Value("${vector.outbox.batch-size:100}") int batchSize,
            @Value("${vector.outbox.send-timeout-seconds:10}") long timeoutSeconds,
            @Value("${vector.outbox.lease-seconds:30}") long leaseSeconds) {
        if (batchSize < 1 || timeoutSeconds < 1 || leaseSeconds <= timeoutSeconds) {
            throw new IllegalArgumentException("Outbox lease must exceed positive send timeout and batch size must be positive");
        }
        this.repository = repository;
        this.sender = sender;
        this.batchSize = batchSize;
        this.sendTimeout = Duration.ofSeconds(timeoutSeconds);
        this.leaseSeconds = leaseSeconds;
    }

    public Mono<Void> dispatchPending() {
        return repository.pendingHeads(batchSize)
                .concatMap(event -> Mono.defer(() -> dispatch(event)).onErrorResume(error -> {
                    log.warn("Outbox event {} retained pending; continuing other users", event.eventId(), error);
                    return Mono.empty();
                })).then();
    }

    private Mono<Void> dispatch(OutboxEvent event) {
        String token = UUID.randomUUID().toString();
        return repository.claim(event.eventId(), token, leaseSeconds).flatMap(claimed -> {
            if (!claimed) return Mono.empty();
            SenderRecord<String, String, String> record = SenderRecord.create(
                    new ProducerRecord<>(event.topic(), event.recordKey(), event.payload()), event.eventId());
            return Mono.defer(() -> sender.send(Mono.just(record)).single()
                    .flatMap(result -> result.exception() == null ? Mono.empty() : Mono.error(result.exception()))
                    .timeout(sendTimeout)
                    .then(repository.markSent(event.eventId(), token)))
                    .onErrorResume(error -> repository.release(event.eventId(), token).then(Mono.error(error)));
        });
    }
}
