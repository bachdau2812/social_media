package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.post.dto.story.event.StoryLikeEventPayload;
import com.dauducbach.clone.modules.post.entity.story.StoryLikeOutboxEntry;
import com.dauducbach.clone.modules.post.repositoty.story.StoryLikeOutboxRepository;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
public class StoryLikeOutboxPublisher {
    private static final String LIKE_EVENT_TOPIC = "like_event";
    private static final int BATCH_SIZE = 50;
    private static final int PUBLISH_CONCURRENCY = 4;
    private static final long LEASE_SECONDS = 30;

    private final StoryLikeOutboxRepository repository;
    private final KafkaSender<String, String> kafkaSender;
    private final AtomicBoolean polling = new AtomicBoolean();

    @Scheduled(fixedDelayString = "${story.like.outbox.poll-delay-ms:1000}")
    public void poll() {
        if (!polling.compareAndSet(false, true)) {
            return;
        }
        publishDue()
                .doOnError(error -> log.error(
                        "|StoryLikeOutboxPublisher|poll|failed|errorType={}",
                        error.getClass().getSimpleName()))
                .onErrorResume(error -> Mono.empty())
                .doFinally(signal -> polling.set(false))
                .subscribe();
    }

    Mono<Void> publishDue() {
        String leaseToken = UUID.randomUUID().toString();
        Instant leaseUntil = Instant.now().plus(LEASE_SECONDS, ChronoUnit.SECONDS);
        return repository.leaseDue(leaseToken, leaseUntil, BATCH_SIZE)
                .collectList()
                .doOnNext(entries -> {
                    if (!entries.isEmpty()) {
                        log.info("|StoryLikeOutboxPublisher|lease|acquired|leaseToken={}|count={}",
                                leaseToken, entries.size());
                    }
                })
                .flatMapMany(Flux::fromIterable)
                .flatMap(entry -> publishOne(entry, leaseToken), PUBLISH_CONCURRENCY)
                .then();
    }

    private Mono<Void> publishOne(StoryLikeOutboxEntry entry, String leaseToken) {
        SenderRecord<String, String, String> record = toSenderRecord(entry);
        log.info(
                "|StoryLikeOutboxPublisher|publish|sending|storyId={}|actorId={}|interactionId={}|attempt={}",
                entry.getStoryId(), entry.getActorId(), entry.getInteractionId(), entry.getAttemptCount() + 1);

        return kafkaSender.send(Mono.just(record))
                .single()
                .flatMap(result -> acknowledgeBrokerResult(entry, leaseToken, result))
                .onErrorResume(error -> scheduleRetry(entry, leaseToken, error));
    }

    private Mono<Void> acknowledgeBrokerResult(
            StoryLikeOutboxEntry entry,
            String leaseToken,
            SenderResult<String> result
    ) {
        if (result.exception() != null) {
            return Mono.error(result.exception());
        }
        var metadata = result.recordMetadata();
        if (metadata == null) {
            return Mono.error(new IllegalStateException("Kafka acknowledgement metadata is missing"));
        }
        log.info(
                "|StoryLikeOutboxPublisher|publish|brokerAcknowledged|interactionId={}|topic={}|partition={}|offset={}",
                entry.getInteractionId(), metadata.topic(), metadata.partition(), metadata.offset());
        return repository.acknowledge(entry.getInteractionId(), leaseToken)
                .flatMap(deleted -> {
                    if (deleted == null || deleted <= 0) {
                        return Mono.error(new IllegalStateException("Outbox lease was lost before acknowledgement"));
                    }
                    log.info("|StoryLikeOutboxPublisher|publish|deleted|interactionId={}", entry.getInteractionId());
                    return Mono.empty();
                });
    }

    private Mono<Void> scheduleRetry(StoryLikeOutboxEntry entry, String leaseToken, Throwable error) {
        long delaySeconds = retryDelaySeconds(entry.getAttemptCount());
        Instant nextAttemptAt = Instant.now().plusSeconds(delaySeconds);
        log.warn(
                "|StoryLikeOutboxPublisher|publish|retryScheduled|interactionId={}|delaySeconds={}|errorType={}",
                entry.getInteractionId(), delaySeconds, error.getClass().getSimpleName());
        return repository.retry(entry.getInteractionId(), leaseToken, nextAttemptAt)
                .flatMap(updated -> {
                    if (updated == null || updated <= 0) {
                        return Mono.error(new IllegalStateException("Outbox lease was lost before retry scheduling"));
                    }
                    return Mono.empty();
                });
    }

    private long retryDelaySeconds(int attemptCount) {
        int exponent = Math.min(Math.max(attemptCount, 0), 8);
        return Math.min(300L, 1L << exponent);
    }

    private SenderRecord<String, String, String> toSenderRecord(StoryLikeOutboxEntry entry) {
        StoryLikeEventPayload payload = new StoryLikeEventPayload(
                entry.getActorId(),
                entry.getStoryId(),
                "STORY",
                entry.getOwnerId(),
                entry.getInteractionId(),
                entry.getCreatedAt()
        );
        JsonObject event = new JsonObject();
        event.addProperty("actorId", payload.actorId());
        event.addProperty("targetId", payload.targetId());
        event.addProperty("targetType", payload.targetType());
        event.addProperty("targetOwnerId", payload.targetOwnerId());
        event.addProperty("interactionId", payload.interactionId());
        event.addProperty("timestamp", payload.timestamp().toString());
        return SenderRecord.create(
                new ProducerRecord<>(LIKE_EVENT_TOPIC, entry.getStoryId(), event.toString()),
                entry.getInteractionId());
    }
}
