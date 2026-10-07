package com.dauducbach.clone.modules.personalization.incoming;

import com.dauducbach.clone.modules.personalization.profile.UserProfileVectorRefreshUseCase;
import com.dauducbach.clone.modules.user.publicapi.UserProfileVectorTopics;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.dauducbach.clone.commons.serialization.JsonPayloadReader;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.concurrent.CompletableFuture;

/** Kafka adapter for user profile changes; user data is loaded through UserProfileQuery by the use case. */
@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserProfileVectorRefreshListener {
    private static final Logger log = LoggerFactory.getLogger(UserProfileVectorRefreshListener.class);

    UserProfileVectorRefreshUseCase refresh;

    @KafkaListener(topics = UserProfileVectorTopics.PROFILE_VECTOR_REFRESH, groupId = "user-service")
    public CompletableFuture<Void> handleProfileVectorRefreshEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = JsonPayloadReader.extractString(json, "userId");
        String source = JsonPayloadReader.extractString(json, "source");
        String eventId = JsonPayloadReader.extractString(json, "eventId");
        // Keep a stable identity for legacy queued events that predate eventId.
        String identity = eventId.isBlank() ? fingerprint(payload) : eventId;
        return refresh.refreshUserVector(userId, identity)
                .doOnSuccess(unused -> log.info("|UserProfileVectorRefreshListener|success|userId={}|source={}", userId, source))
                .doOnError(error -> log.error("|UserProfileVectorRefreshListener|failed|userId={}|source={}|error={}",
                        userId, source, error.getMessage()))
                .toFuture();
    }

    /** Compatibility entry point for legacy create payloads; current SQL remains authoritative. */
    public Mono<Void> refreshCreatedUserVector(JsonObject profileJson) {
        if (profileJson == null) return Mono.empty();
        String userId = JsonPayloadReader.extractString(profileJson, "userId");
        return refresh.refreshUserVector(userId, "profile-create:" + userId);
    }

    private String fingerprint(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException(error);
        }
    }
}
