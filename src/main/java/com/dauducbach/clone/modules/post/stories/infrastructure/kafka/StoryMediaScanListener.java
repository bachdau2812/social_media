package com.dauducbach.clone.modules.post.stories.infrastructure.kafka;

import com.dauducbach.clone.modules.post.stories.publishing.StoryMediaScanRequest;
import com.dauducbach.clone.modules.post.stories.publishing.StoryMediaScanService;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.dauducbach.clone.commons.serialization.JsonPayloadReader;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

@Component
public class StoryMediaScanListener {
    private static final Logger log = LoggerFactory.getLogger(StoryMediaScanListener.class);

    private final StoryMediaScanService storyMediaScanService;

    public StoryMediaScanListener(StoryMediaScanService storyMediaScanService) {
        this.storyMediaScanService = storyMediaScanService;
    }

    @KafkaListener(topics = "check_story_media_event", groupId = "user-service")
    public CompletableFuture<Void> handle(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        StoryMediaScanRequest request = new StoryMediaScanRequest(
                JsonPayloadReader.extractString(json, "storyId"),
                JsonPayloadReader.extractString(json, "userId"),
                JsonPayloadReader.extractString(json, "mediaUrl"),
                JsonPayloadReader.extractString(json, "publicId"),
                JsonPayloadReader.extractString(json, "mediaType"));
        log.info("|StoryMediaScanListener|received|storyId={}|userId={}|publicId={}|mediaType={}",
                request.storyId(), request.userId(), request.publicId(), request.mediaType());

        if (request.storyId().isBlank() || request.userId().isBlank() || request.mediaUrl().isBlank()) {
            log.warn("|StoryMediaScanListener|missing required data|storyId={}|userId={}",
                    request.storyId(), request.userId());
            return CompletableFuture.completedFuture(null);
        }
        return Mono.defer(() -> storyMediaScanService.scan(request))
                .doOnError(error -> log.error("|StoryMediaScanListener|failed|storyId={}|error={}",
                        request.storyId(), error.getMessage()))
                .toFuture();
    }
}
