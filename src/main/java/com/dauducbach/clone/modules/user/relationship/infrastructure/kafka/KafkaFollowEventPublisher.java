package com.dauducbach.clone.modules.user.relationship.infrastructure.kafka;

import com.dauducbach.clone.modules.user.relationship.application.FollowEventPublisher;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Component
@RequiredArgsConstructor
public class KafkaFollowEventPublisher implements FollowEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaFollowEventPublisher.class);
    private static final String FOLLOW_TOPIC = "follow_event";
    private static final String UNFOLLOW_TOPIC = "un_follow_event";

    private final KafkaSender<String, String> kafkaSender;

    @Override
    public Mono<Void> followed(String followerId, String followingId) {
        return publish(FOLLOW_TOPIC, followerId, followerId, followingId);
    }

    @Override
    public Mono<Void> unfollowed(String followerId, String followingId) {
        return publish(UNFOLLOW_TOPIC, followerId, followerId, followingId);
    }

    private Mono<Void> publish(String topic, String key, String followerId, String followingId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("followerId", followerId);
        payload.addProperty("followingId", followingId);
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(topic, key, payload.toString()), topic);
        return kafkaSender.send(Mono.just(record))
                .flatMap(result -> result.exception() == null
                        ? Mono.just(result)
                        : Mono.error(result.exception()))
                .doOnError(error -> log.error("|KafkaFollowEventPublisher|publish|topic={}|error={}",
                        topic, error.getMessage()))
                .then();
    }
}
