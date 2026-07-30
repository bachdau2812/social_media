package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class PostEventBroadcast {
    private static final Logger log = LoggerFactory.getLogger(PostEventBroadcast.class);

    private final PostVectorService postVectorService;

    @KafkaListener(topics = "post_upload_event", groupId = "post-service")
    public CompletableFuture<Void> handlePostEmbeddingEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String postId = resolveField(payloadJson, "postId");
        String content = KafkaUtils.extractString(payloadJson, "content");
        if (postId.isBlank() || content.isBlank()) {
            log.warn(
                    "|PostEventBroadcast|handlePostEmbeddingEvent|missing data|hasPostId={}|hasContent={}",
                    !postId.isBlank(), !content.isBlank());
            return CompletableFuture.completedFuture(null);
        }

        return postVectorService.processPostEmbedding(postId, content)
                .doOnError(error -> log.error(
                        "|PostEventBroadcast|handlePostEmbeddingEvent|failed|postId={}|error={}",
                        postId, error.getMessage()))
                .toFuture();
    }

    private String resolveField(JsonObject payloadJson, String fieldName) {
        String value = KafkaUtils.extractString(payloadJson, fieldName);
        if (!value.isBlank()) {
            return value;
        }
        return "postId".equals(fieldName)
                ? KafkaUtils.extractString(payloadJson, "post_id")
                : value;
    }
}