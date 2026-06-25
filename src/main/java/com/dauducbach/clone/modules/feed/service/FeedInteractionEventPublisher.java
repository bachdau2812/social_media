package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.constant.FeedTopics;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class FeedInteractionEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(FeedInteractionEventPublisher.class);

    KafkaSender<String, String> kafkaSender;

    public Mono<Void> publishInteraction(String userId, String postId, String action, String sourceEventId) {
        if (userId == null || userId.isBlank() || postId == null || postId.isBlank() || action == null || action.isBlank()) {
            return Mono.empty();
        }

        JsonObject payload = new JsonObject();
        payload.addProperty("eventId", UUID.randomUUID().toString());
        payload.addProperty("userId", userId);
        payload.addProperty("postId", postId);
        payload.addProperty("action", action.trim().toUpperCase());
        payload.addProperty("sourceEventId", sourceEventId);
        payload.addProperty("createdAt", Instant.now().toString());

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(FeedTopics.USER_INTERACTION_EVENTS, userId, payload.toString()),
                FeedTopics.USER_INTERACTION_EVENTS
        );

        return kafkaSender.send(Mono.just(record))
                .doOnComplete(() -> log.info("|FeedInteractionEventPublisher|publishInteraction|sent|userId={}|postId={}|action={}",
                        userId, postId, action))
                .doOnError(error -> log.error("|FeedInteractionEventPublisher|publishInteraction|failed|userId={}|postId={}|error={}",
                        userId, postId, error.getMessage()))
                .then();
    }
}
