package com.dauducbach.clone.modules.user.profile.infrastructure.kafka;

import com.dauducbach.clone.modules.user.profile.application.AvatarMediaReviewRequested;
import com.dauducbach.clone.modules.user.profile.application.AvatarUploadUseCase;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.dauducbach.clone.commons.serialization.JsonPayloadReader;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
public class AvatarScanEventListener {
    private static final Logger log = LoggerFactory.getLogger(AvatarScanEventListener.class);
    private final AvatarUploadUseCase avatarUploadUseCase;

    @KafkaListener(topics = "check_avatar_media_event", groupId = "user-service")
    public CompletableFuture<Void> handle(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        AvatarMediaReviewRequested request = new AvatarMediaReviewRequested(
                JsonPayloadReader.extractString(json, "userId"),
                JsonPayloadReader.extractString(json, "avatarUrl"),
                JsonPayloadReader.extractString(json, "publicId"));
        if (request.userId().isBlank() || request.avatarUrl().isBlank()) {
            log.warn("|AvatarScanEventListener|handle|missing data");
            return CompletableFuture.completedFuture(null);
        }
        return avatarUploadUseCase.reviewAvatar(request).toFuture();
    }
}
