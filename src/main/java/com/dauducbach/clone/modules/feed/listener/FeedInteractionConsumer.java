package com.dauducbach.clone.modules.feed.listener;

import com.dauducbach.clone.modules.feed.dto.event.FeedInteractionEvent;
import com.dauducbach.clone.modules.personalization.publicapi.CanonicalInteractionPosition;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceInteraction;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceInteractionProcessor;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;
import java.time.Duration;

@Component
@ConditionalOnProperty(name = "vector.interaction.consumer.enabled", havingValue = "true")
public class FeedInteractionConsumer implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(FeedInteractionConsumer.class);
    private final ReceiverOptions<String, String> options;
    private final PreferenceInteractionProcessor processing;
    private final String generation;
    private final int partitionCount;
    private volatile Disposable subscription;

    public FeedInteractionConsumer(@Qualifier("feedInteractionReceiverOptions") ReceiverOptions<String, String> options,
            PreferenceInteractionProcessor processing,
            @Value("${vector.interaction.consumer.generation:v1}") String generation,
            @Value("${vector.interaction.consumer.partition-count:1}") int partitionCount) {
        if (generation == null || generation.isBlank() || partitionCount < 1)
            throw new IllegalArgumentException("Canonical generation and partition count are required");
        this.options = options; this.processing = processing; this.generation = generation; this.partitionCount = partitionCount;
    }

    public Flux<Void> consume(KafkaReceiver<String, String> receiver) {
        return receiver.receive(1)
                .groupBy(record -> record.receiverOffset().topicPartition(), partitionCount)
                .flatMap(partition -> partition.concatMap(this::applyAndCommit, 1), partitionCount, 1);
    }

    private Mono<Void> applyAndCommit(ReceiverRecord<String, String> record) {
        return Mono.defer(() -> {
            if (record.partition() >= partitionCount)
                return Mono.error(new IllegalStateException("Canonical partition count changed"));
            FeedInteractionEvent event = FeedInteractionEvent.fromJson(GsonUtils.fromString(record.value()));
            if (!event.userId().equals(record.key()))
                return Mono.error(new IllegalArgumentException("Canonical Kafka key must equal userId"));
            PreferenceInteraction interaction = toPreferenceInteraction(event);
            CanonicalInteractionPosition position = new CanonicalInteractionPosition(
                    record.topic(), generation, record.partition(), record.offset());
            return processing.apply(interaction, position)
                    .then(Mono.defer(() -> record.receiverOffset().commit()));
        }).retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofMinutes(1))
                .doBeforeRetry(retry -> log.error("Canonical interaction parked/retrying; topic={} partition={} offset={} attempt={} cause={}",
                        record.topic(), record.partition(), record.offset(), retry.totalRetries() + 1, retry.failure().toString())));
    }

    static PreferenceInteraction toPreferenceInteraction(FeedInteractionEvent event) {
        return new PreferenceInteraction(event.eventId(), event.userId(), event.postId(),
                event.action(), event.sourceId(), event.occurredAt());
    }

    @Override public synchronized void start() {
        if (isRunning()) return;
        subscription = Flux.defer(() -> consume(KafkaReceiver.create(options)))
                // Record failures remain inside concatMap. Only receiver/session failures restart the session.
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(retry -> log.error("Canonical receiver session failed; restarting: {}", retry.failure().toString())))
                .subscribe(unused -> {}, error -> log.error("Canonical receiver terminated", error));
    }
    @Override public synchronized void stop() { if (subscription != null) subscription.dispose(); subscription = null; }
    @Override public void stop(Runnable callback) { stop(); callback.run(); }
    @Override public boolean isRunning() { return subscription != null && !subscription.isDisposed(); }
    @Override public boolean isAutoStartup() { return true; }
    @Override public int getPhase() { return Integer.MAX_VALUE; }
}
