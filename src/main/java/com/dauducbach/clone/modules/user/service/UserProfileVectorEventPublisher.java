package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.constant.UserProfileVectorTopics;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.utils.GsonUtils;
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
public class UserProfileVectorEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(UserProfileVectorEventPublisher.class);

    KafkaSender<String, String> kafkaSender;

    public Mono<Void> publishRefreshEvent(String userId, String source, String operation, String resourceId) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }

        JsonObject payload = basePayload(userId, source, operation, resourceId);
        return sendPayload(userId, source, operation, payload);
    }

    public Mono<Void> publishRefreshEventForCreatedUser(String userId,
                                                        String source,
                                                        String operation,
                                                        String resourceId,
                                                        UserDetails userDetails) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }

        JsonObject payload = basePayload(userId, source, operation, resourceId);
        JsonObject profile = new JsonObject();
        if (userDetails != null) {
            profile.addProperty("userId", userDetails.getUserId());
            profile.addProperty("username", userDetails.getUsername());
            profile.addProperty("hometown", userDetails.getHometown());
            profile.addProperty("livingIn", userDetails.getLivingIn());
            profile.addProperty("sex", userDetails.getSex());
            profile.addProperty("dob", userDetails.getDob() == null ? null : userDetails.getDob().toString());
            profile.add("hobbyList", GsonUtils.getGson().toJsonTree(userDetails.getHobbyList()));
        }
        payload.add("profile", profile);
        return sendPayload(userId, source, operation, payload);
    }

    private JsonObject basePayload(String userId, String source, String operation, String resourceId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("eventId", UUID.randomUUID().toString());
        payload.addProperty("userId", userId);
        payload.addProperty("source", source);
        payload.addProperty("operation", operation);
        payload.addProperty("resourceId", resourceId);
        payload.addProperty("createdAt", Instant.now().toString());
        return payload;
    }

    private Mono<Void> sendPayload(String userId, String source, String operation, JsonObject payload) {
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(UserProfileVectorTopics.PROFILE_VECTOR_REFRESH, userId, payload.toString()),
                UserProfileVectorTopics.PROFILE_VECTOR_REFRESH
        );

        return kafkaSender.send(Mono.just(record))
                .doOnComplete(() -> log.info("|UserProfileVectorEventPublisher|publishRefreshEvent|sent|userId={}|source={}|operation={}",
                        userId, source, operation))
                .doOnError(error -> log.error("|UserProfileVectorEventPublisher|publishRefreshEvent|failed|userId={}|source={}|error={}",
                        userId, source, error.getMessage()))
                .then();
    }
}
