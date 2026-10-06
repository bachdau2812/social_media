package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.constant.FeedTopics;
import com.dauducbach.clone.modules.feed.dto.event.FeedInteractionEvent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class FeedInteractionEventPublisher {
    private final KafkaSender<String, String> kafkaSender;

    public Mono<Void> publishInteraction(String userId, String postId, String action, String sourceId, Instant occurredAt) {
        return Mono.defer(() -> publish(new FeedInteractionEvent(action + ":" + sourceId,
                userId, postId, action, sourceId, occurredAt)));
    }

    public Mono<Void> publish(FeedInteractionEvent event) {
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(FeedTopics.USER_INTERACTION_EVENTS, event.userId(), event.toJson().toString()), event.eventId());
        return kafkaSender.send(Mono.just(record)).single()
                .flatMap(result -> result.exception() == null ? Mono.empty() : Mono.error(result.exception()));
    }
}
