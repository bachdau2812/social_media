package com.dauducbach.clone.modules.user.profile.infrastructure.kafka;

import com.dauducbach.clone.modules.user.profile.application.AvatarMediaEventPublisher;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Component
public class AvatarMediaKafkaPublisher implements AvatarMediaEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(AvatarMediaKafkaPublisher.class);
    private static final String CHECK_AVATAR_MEDIA_EVENT = "check_avatar_media_event";
    private static final String AVATAR_UPDATE_EVENT = "avatar_update_event";

    private final KafkaSender<String, String> kafkaSender;

    public AvatarMediaKafkaPublisher(KafkaSender<String, String> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    @Override
    public Mono<Void> requestAvatarScan(String userId, String avatarUrl, String publicId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", userId);
        payload.addProperty("avatarUrl", avatarUrl);
        payload.addProperty("publicId", publicId);
        return send(CHECK_AVATAR_MEDIA_EVENT, userId, payload);
    }

    @Override
    public Mono<Void> publishAvatarUpdated(String userId, String avatarUrl, String mediaId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", userId);
        payload.addProperty("avatarUrl", avatarUrl);
        payload.addProperty("mediaId", mediaId);
        return send(AVATAR_UPDATE_EVENT, userId, payload);
    }

    private Mono<Void> send(String topic, String key, JsonObject payload) {
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(topic, key, payload.toString()), topic);
        return kafkaSender.send(Mono.just(record))
                .flatMap(result -> result.exception() == null
                        ? Mono.just(result)
                        : Mono.error(result.exception()))
                .doOnError(error -> log.error("|AvatarMediaKafkaPublisher|send|topic={}|error={}",
                        topic, error.getMessage()))
                .then();
    }
}
