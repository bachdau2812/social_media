package com.dauducbach.clone.modules.feed.listener;

import com.dauducbach.clone.commons.constant.EntityType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.dauducbach.clone.modules.feed.constant.FeedTopics;
import com.dauducbach.clone.modules.feed.service.FeedInteractionEventPublisher;
import com.dauducbach.clone.modules.feed.service.FeedService;
import com.dauducbach.clone.modules.user.publicapi.UserRelationshipQuery;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.dauducbach.clone.commons.serialization.JsonPayloadReader;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Component
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class FeedEventListener {
    private static final Logger log = LoggerFactory.getLogger(FeedEventListener.class);

    FeedService feedService;
    FeedInteractionEventPublisher interactionEventPublisher;
    UserRelationshipQuery userRelationshipQuery;

    @KafkaListener(topics = FeedTopics.POST_UPLOAD_EVENT, groupId = "feed-service")
    public CompletableFuture<Void> handlePostUploadEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String postId = resolvePostId(json);
        String userId = JsonPayloadReader.extractString(json, "userId");

        if (postId.isBlank() || userId.isBlank()) {
            log.warn("|FeedEventListener|handlePostUploadEvent|missing data|hasPostId={}|hasUserId={}",
                    !postId.isBlank(), !userId.isBlank());
            return CompletableFuture.completedFuture(null);
        }

        return userRelationshipQuery.getFollowerIdsForFeedBroadcast(userId)
                .concatMap(followerId -> feedService.appendPostToUserFeed(followerId, postId, Instant.now()))
                .then()
                .doOnSuccess(unused -> log.info("|FeedEventListener|handlePostUploadEvent|broadcasted|postId={}|userId={}",
                        postId, userId))
                .doOnError(error -> log.error("|FeedEventListener|handlePostUploadEvent|failed|postId={}|error={}",
                        postId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = FeedTopics.LIKE_EVENT, groupId = "feed-service")
    public CompletableFuture<Void> handleLikeEvent(ConsumerRecord<String, String> record) {
        JsonObject json = GsonUtils.fromString(record.value());
        String actorId = JsonPayloadReader.extractString(json, "actorId");
        String targetType = JsonPayloadReader.extractString(json, "targetType").toUpperCase();
        String postId = EntityType.POST.name().equals(targetType)
                ? JsonPayloadReader.extractString(json, "targetId")
                : JsonPayloadReader.extractString(json, "postId");

        return interactionEventPublisher.publishInteraction(actorId, postId, "LIKE", sourceId(json, "likeId", record), occurredAt(json, record))
                .doOnError(error -> log.error("|FeedEventListener|handleLikeEvent|failed|actorId={}|postId={}|error={}",
                        actorId, postId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = FeedTopics.COMMENT_SUCCESS_EVENT, groupId = "feed-service")
    public CompletableFuture<Void> handleCommentSuccessEvent(ConsumerRecord<String, String> record) {
        JsonObject json = GsonUtils.fromString(record.value());
        String userId = JsonPayloadReader.extractString(json, "userId");
        String postId = JsonPayloadReader.extractString(json, "postId");
        String commentId = JsonPayloadReader.extractString(json, "commentId");

        return interactionEventPublisher.publishInteraction(userId, postId, "COMMENT", sourceId(json, "commentId", record), occurredAt(json, record))
                .doOnError(error -> log.error("|FeedEventListener|handleCommentSuccessEvent|failed|userId={}|postId={}|error={}",
                        userId, postId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = FeedTopics.REPOST_EVENT, groupId = "feed-service")
    public CompletableFuture<Void> handleRepostEvent(ConsumerRecord<String, String> record) {
        JsonObject json = GsonUtils.fromString(record.value());
        return interactionEventPublisher.publishInteraction(JsonPayloadReader.extractString(json, "actorId"),
                JsonPayloadReader.extractString(json, "postId"), "REPOST", sourceId(json, "repostId", record),
                occurredAt(json, record)).toFuture();
    }

    private String sourceId(JsonObject json, String field, ConsumerRecord<String, String> record) {
        String persistedId = JsonPayloadReader.extractString(json, field);
        return persistedId.isBlank()
                ? "LEGACY:" + record.topic() + ":" + record.partition() + ":" + record.offset()
                : persistedId;
    }

    private Instant occurredAt(JsonObject json, ConsumerRecord<String, String> record) {
        String original = JsonPayloadReader.extractString(json, "occurredAt");
        if (original.isBlank()) original = JsonPayloadReader.extractString(json, "timestamp");
        return original.isBlank() ? Instant.ofEpochMilli(Math.max(0, record.timestamp())) : Instant.parse(original);
    }

    private String resolvePostId(JsonObject json) {
        String postId = JsonPayloadReader.extractString(json, "postId");
        if (!postId.isBlank()) {
            return postId;
        }
        return JsonPayloadReader.extractString(json, "post_id");
    }
}
