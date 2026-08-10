package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.post.service.comment.CommentMediaModerationOrchestrator;
import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class ImageScanWorker {
    private static final Logger log = LoggerFactory.getLogger(ImageScanWorker.class);

    private final PostMediaModerationOrchestrator postOrchestrator;
    private final CommentMediaModerationOrchestrator commentOrchestrator;

    @KafkaListener(topics = "check_media_event", groupId = "post-service")
    public CompletableFuture<Void> handlePostScanEvent(@Payload String payload) {
        try {
            JsonObject payloadJson = GsonUtils.fromString(payload);
            String postId = KafkaUtils.extractString(payloadJson, "postId");
            String userId = KafkaUtils.extractString(payloadJson, "userId");
            List<PostMediaScanItem> items = extractPostScanItems(payloadJson);
            if (postId.isBlank() || userId.isBlank() || items.isEmpty()) {
                log.warn(
                        "|ImageScanWorker|handlePostScanEvent|missing data|postId={}|userId={}|itemCount={}",
                        postId, userId, items.size());
                return CompletableFuture.completedFuture(null);
            }
            return postOrchestrator.process(postId, userId, items)
                    .doOnError(error -> log.error(
                            "|ImageScanWorker|handlePostScanEvent|failed|postId={}|error={}",
                            postId, error.getMessage()))
                    .toFuture();
        } catch (RuntimeException error) {
            log.error("|ImageScanWorker|handlePostScanEvent|invalid payload|error={}", error.getMessage());
            return CompletableFuture.failedFuture(error);
        }
    }

    @KafkaListener(topics = "check_comment_media_event", groupId = "post-service")
    public CompletableFuture<Void> handleCommentScanEvent(@Payload String payload) {
        try {
            JsonObject payloadJson = GsonUtils.fromString(payload);
            String commentId = KafkaUtils.extractString(payloadJson, "commentId");
            String postId = KafkaUtils.extractString(payloadJson, "postId");
            List<MediaUploadRequest> mediaList = extractMediaList(payloadJson);
            if (commentId.isBlank() || postId.isBlank() || mediaList.isEmpty()) {
                log.warn(
                        "|ImageScanWorker|handleCommentScanEvent|missing data|commentId={}|postId={}|mediaCount={}",
                        commentId, postId, mediaList.size());
                return CompletableFuture.completedFuture(null);
            }
            return commentOrchestrator.process(commentId, postId, mediaList)
                    .doOnError(error -> log.error(
                            "|ImageScanWorker|handleCommentScanEvent|failed|commentId={}|error={}",
                            commentId, error.getMessage()))
                    .toFuture();
        } catch (RuntimeException error) {
            log.error("|ImageScanWorker|handleCommentScanEvent|invalid payload|error={}", error.getMessage());
            return CompletableFuture.failedFuture(error);
        }
    }

    private List<PostMediaScanItem> extractPostScanItems(JsonObject payloadJson) {
        List<PostMediaScanItem> items = new ArrayList<>();
        if (payloadJson.has("items") && payloadJson.get("items").isJsonArray()) {
            JsonArray array = payloadJson.getAsJsonArray("items");
            for (JsonElement element : array) {
                if (!element.isJsonObject()) {
                    continue;
                }
                JsonObject item = element.getAsJsonObject();
                int fallbackOrder = items.size() + 1;
                Long order = KafkaUtils.extractLong(item, "orderNumber");
                items.add(PostMediaScanItem.builder()
                        .orderNumber(order == null || order <= 0
                                ? fallbackOrder
                                : Math.toIntExact(order))
                        .secureUrl(KafkaUtils.extractString(item, "secureUrl"))
                        .publicId(KafkaUtils.extractString(item, "publicId"))
                        .resourceType(KafkaUtils.extractString(item, "resourceType"))
                        .caption(KafkaUtils.extractString(item, "caption"))
                        .musicId(KafkaUtils.extractString(item, "musicId"))
                        .musicStart(KafkaUtils.extractLong(item, "musicStart"))
                        .musicEnd(KafkaUtils.extractLong(item, "musicEnd"))
                        .build());
            }
        }
        return items;
    }

    private List<MediaUploadRequest> extractMediaList(JsonObject payloadJson) {
        List<MediaUploadRequest> mediaList = new ArrayList<>();
        if (payloadJson.has("media") && payloadJson.get("media").isJsonArray()) {
            JsonArray array = payloadJson.getAsJsonArray("media");
            for (JsonElement element : array) {
                if (!element.isJsonObject()) {
                    continue;
                }
                JsonObject item = element.getAsJsonObject();
                mediaList.add(MediaUploadRequest.builder()
                        .secureUrl(KafkaUtils.extractString(item, "secureUrl"))
                        .publicId(KafkaUtils.extractString(item, "publicId"))
                        .resourceType(KafkaUtils.extractString(item, "resourceType"))
                        .build());
            }
        }
        return mediaList;
    }
}
