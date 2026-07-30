package com.dauducbach.clone.modules.feed.listener;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.modules.feed.constant.FeedTopics;
import com.dauducbach.clone.modules.feed.service.FeedInteractionEventPublisher;
import com.dauducbach.clone.modules.feed.service.FeedService;
import com.dauducbach.clone.modules.feed.service.FeedVectorService;
import com.dauducbach.clone.modules.user.service.UserFollowerService;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
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
    FeedVectorService feedVectorService;
    FeedInteractionEventPublisher interactionEventPublisher;
    UserFollowerService userFollowerService;

    @KafkaListener(topics = FeedTopics.POST_UPLOAD_EVENT, groupId = "feed-service")
    public CompletableFuture<Void> handlePostUploadEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String postId = resolvePostId(json);
        String userId = KafkaUtils.extractString(json, "userId");

        if (postId.isBlank() || userId.isBlank()) {
            log.warn("|FeedEventListener|handlePostUploadEvent|missing data|hasPostId={}|hasUserId={}",
                    !postId.isBlank(), !userId.isBlank());
            return CompletableFuture.completedFuture(null);
        }

        return userFollowerService.getFollowerIdsForFeedBroadcast(userId)
                .concatMap(followerId -> feedService.appendPostToUserFeed(followerId, postId, Instant.now()))
                .then()
                .doOnSuccess(unused -> log.info("|FeedEventListener|handlePostUploadEvent|broadcasted|postId={}|userId={}",
                        postId, userId))
                .doOnError(error -> log.error("|FeedEventListener|handlePostUploadEvent|failed|postId={}|error={}",
                        postId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = FeedTopics.LIKE_EVENT, groupId = "feed-service")
    public CompletableFuture<Void> handleLikeEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String actorId = KafkaUtils.extractString(json, "actorId");
        String targetType = KafkaUtils.extractString(json, "targetType").toUpperCase();
        String postId = EntityType.POST.name().equals(targetType)
                ? KafkaUtils.extractString(json, "targetId")
                : KafkaUtils.extractString(json, "postId");

        return interactionEventPublisher.publishInteraction(actorId, postId, "LIKE", KafkaUtils.extractString(json, "targetId"))
                .doOnError(error -> log.error("|FeedEventListener|handleLikeEvent|failed|actorId={}|postId={}|error={}",
                        actorId, postId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = FeedTopics.COMMENT_SUCCESS_EVENT, groupId = "feed-service")
    public CompletableFuture<Void> handleCommentSuccessEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(json, "userId");
        String postId = KafkaUtils.extractString(json, "postId");
        String commentId = KafkaUtils.extractString(json, "commentId");

        return interactionEventPublisher.publishInteraction(userId, postId, "COMMENT", commentId)
                .doOnError(error -> log.error("|FeedEventListener|handleCommentSuccessEvent|failed|userId={}|postId={}|error={}",
                        userId, postId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = FeedTopics.USER_INTERACTION_EVENTS, groupId = "feed-service")
    public CompletableFuture<Void> handleUserInteractionEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(json, "userId");
        String postId = KafkaUtils.extractString(json, "postId");
        String action = KafkaUtils.extractString(json, "action");

        return feedVectorService.updateShortTermVector(userId, postId, action)
                .doOnError(error -> log.error("|FeedEventListener|handleUserInteractionEvent|failed|userId={}|postId={}|error={}",
                        userId, postId, error.getMessage()))
                .toFuture();
    }

    private String resolvePostId(JsonObject json) {
        String postId = KafkaUtils.extractString(json, "postId");
        if (!postId.isBlank()) {
            return postId;
        }
        return KafkaUtils.extractString(json, "post_id");
    }
}
