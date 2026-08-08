package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.commons.constant.PostNotificationCacheKeys;
import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.request.NotificationRequest;
import com.dauducbach.clone.modules.notification.entity.NotificationTemplates;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.dauducbach.clone.modules.post.service.comment.CommentService;
import com.dauducbach.clone.modules.post.service.post.LikeService;
import com.dauducbach.clone.modules.post.service.post.PostService;
import com.dauducbach.clone.modules.user.dto.response.FollowerListResponse;
import com.dauducbach.clone.modules.user.service.UserFollowerService;
import com.dauducbach.clone.modules.user.service.UserIdentityQueryService;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Service
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
@RequiredArgsConstructor
public class PushModuleNotificationHandler {
    private static final Logger log = LoggerFactory.getLogger(PushModuleNotificationHandler.class);
    private static final long MANY_INTERACTIONS_THRESHOLD = 10L;
    private static final int FOLLOWER_PAGE_SIZE = 100;

    NotificationService notificationService;
    NotificationTemplatesRepository notificationTemplatesRepository;
    ReactiveRedisTemplate<String, Object> redisTemplate;
    UserFollowerService userFollowerService;
    UserIdentityQueryService userIdentityQueryService;
    PostService postService;
    CommentService commentService;
    LikeService likeService;

    @KafkaListener(topics = "post_upload_event", groupId = "notification-service")
    public CompletableFuture<Void> handlePostUploadEvent(@Payload Object payload) {
        JsonObject payloadJson = toJsonObject(payload);
        String postId = firstString(payloadJson, "postId", "post_id");
        String userId = firstString(payloadJson, "userId", "user_id");
        String content = firstString(payloadJson, "content");

        if (postId.isBlank() || userId.isBlank()) {
            log.warn("|PushModuleNotificationHandler|handlePostUploadEvent|missing data|postId={}|userId={}", postId, userId);
            return CompletableFuture.completedFuture(null);
        }

        Map<String, String> metadata = baseMetadata(userId, postId, EntityType.POST.name());
        metadata.put("CONTENT", content);

        return enrichActorUsername(userId, metadata)
                .then(getFollowersOfUser(userId))
                .flatMap(followers -> sendPush(UserActionType.NEW_POST, userId, postId, EntityType.POST.name(), followers, metadata, true))
                .doOnSuccess(v -> log.info("|PushModuleNotificationHandler|handlePostUploadEvent|completed|postId={}|userId={}", postId, userId))
                .doOnError(error -> log.error("|PushModuleNotificationHandler|handlePostUploadEvent|failed|postId={}|error={}", postId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = "comment_success_event", groupId = "notification-service")
    public CompletableFuture<Void> handleCommentSuccessEvent(@Payload Object payload) {
        JsonObject payloadJson = toJsonObject(payload);
        String commentId = firstString(payloadJson, "commentId", "comment_id");
        String userId = firstString(payloadJson, "userId", "user_id");
        String postId = firstString(payloadJson, "postId", "post_id");
        String parentId = firstString(payloadJson, "parentId", "parent_id");
        String content = firstString(payloadJson, "content");

        if (commentId.isBlank() || userId.isBlank() || postId.isBlank()) {
            log.warn("|PushModuleNotificationHandler|handleCommentSuccessEvent|missing data|commentId={}|userId={}|postId={}",
                    commentId, userId, postId);
            return CompletableFuture.completedFuture(null);
        }

        Map<String, String> metadata = baseMetadata(userId, commentId, EntityType.COMMENT.name());
        metadata.put("POST_ID", postId);
        metadata.put("COMMENT_ID", commentId);
        metadata.put("PARENT_COMMENT_ID", parentId);
        metadata.put("COMMENT", content);
        metadata.put("REPLY", content);

        return enrichActorUsername(userId, metadata)
                .then(enrichPostContent(postId, metadata))
                .then(sendCommentNotifications(userId, postId, commentId, parentId, metadata))
                .doOnSuccess(v -> log.info("|PushModuleNotificationHandler|handleCommentSuccessEvent|completed|commentId={}|postId={}", commentId, postId))
                .doOnError(error -> log.error("|PushModuleNotificationHandler|handleCommentSuccessEvent|failed|commentId={}|error={}", commentId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = "like_event", groupId = "notification-service")
    public CompletableFuture<Void> handleLikeEvent(@Payload Object payload) {
        JsonObject payloadJson = toJsonObject(payload);
        String actorId = firstString(payloadJson, "actorId", "actor_id");
        String targetId = firstString(payloadJson, "targetId", "target_id");
        String targetType = firstString(payloadJson, "targetType", "target_type");
        String targetOwnerId = firstString(payloadJson, "targetOwnerId", "target_owner_id");
        String postId = firstString(payloadJson, "postId", "post_id");
        String interactionId = firstString(payloadJson, "interactionId", "interaction_id");
        Long likeCount = firstLong(payloadJson, "likeCount", "like_count");

        if (actorId.isBlank() || targetId.isBlank() || targetType.isBlank()) {
            log.warn("|PushModuleNotificationHandler|handleLikeEvent|missing data|actorId={}|targetId={}|targetType={}",
                    actorId, targetId, targetType);
            return CompletableFuture.completedFuture(null);
        }

        Mono<Void> notification = switch (targetType.trim().toUpperCase()) {
            case "POST" -> handlePostLike(actorId, targetId, targetOwnerId, likeCount);
            case "COMMENT" -> handleCommentLike(actorId, targetId, targetOwnerId, postId);
            case "STORY" -> handleStoryLike(actorId, targetId, targetOwnerId, interactionId);
            default -> Mono.empty();
        };

        return notification
                .doOnSuccess(v -> log.info("|PushModuleNotificationHandler|handleLikeEvent|completed|targetId={}|targetType={}", targetId, targetType))
                .doOnError(error -> log.error("|PushModuleNotificationHandler|handleLikeEvent|failed|targetId={}|error={}", targetId, error.getMessage()))
                .toFuture();
    }

    private Mono<Void> sendCommentNotifications(String actorId, String postId, String commentId, String parentId, Map<String, String> metadata) {
        Mono<String> postOwner = getPostOwner(postId).cache();
        Mono<String> parentOwner = parentId.isBlank()
                ? Mono.just("")
                : getCommentOwner(parentId).cache();
        Mono<Void> notifyPostOwner = postOwner.flatMap(ownerId -> commentService.countCommentsByPostId(postId)
                .defaultIfEmpty(0L)
                .flatMap(commentCount -> {
                    metadata.put("COMMENT_COUNT", String.valueOf(Math.max(commentCount - 1, 0)));
                    UserActionType actionType = commentCount >= MANY_INTERACTIONS_THRESHOLD
                            ? UserActionType.COMMENTS
                            : UserActionType.COMMENT;
                    return sendPush(actionType, actorId, commentId, EntityType.COMMENT.name(), List.of(ownerId), metadata, true);
                }));

        Mono<Void> notifyParentOwner = parentId.isBlank()
                ? Mono.empty()
                : parentOwner
                .flatMap(parentOwnerId -> sendPush(UserActionType.REPLY_COMMENT, actorId, commentId, EntityType.COMMENT.name(), List.of(parentOwnerId), metadata, true));

        Mono<Void> notifyInteractedPeople = Mono.zip(postOwner.defaultIfEmpty(""), parentOwner.defaultIfEmpty(""))
                .flatMap(owners -> getPostCommentersExcludingUsers(postId, List.of(owners.getT1(), owners.getT2())))
                .flatMap(recipients -> sendPush(UserActionType.COMMENT_OTHER_INTERACT_PEOPLE, actorId, commentId, EntityType.COMMENT.name(), recipients, metadata, true));

        return Mono.when(notifyPostOwner, notifyParentOwner, notifyInteractedPeople).then();
    }

    private Mono<Void> handlePostLike(String actorId, String postId, String targetOwnerId, Long eventLikeCount) {
        Map<String, String> metadata = baseMetadata(actorId, postId, EntityType.POST.name());
        metadata.put("POST_ID", postId);

        Mono<String> owner = targetOwnerId.isBlank()
                ? getPostOwner(postId)
                : Mono.just(targetOwnerId);
        Mono<String> postOwner = owner.cache();

        Mono<Long> count = eventLikeCount == null
                ? likeService.countLikes(postId, EntityType.POST.name()).defaultIfEmpty(0L)
                : Mono.just(eventLikeCount);

        return enrichActorUsername(actorId, metadata)
                .then(enrichPostContent(postId, metadata))
                .then(Mono.defer(() -> {
                    Mono<Void> notifyOwner = postOwner.flatMap(ownerId -> count.flatMap(likeCount -> {
                        metadata.put("LIKE_COUNT", String.valueOf(Math.max(likeCount - 1, 0)));
                        UserActionType actionType = likeCount >= MANY_INTERACTIONS_THRESHOLD
                                ? UserActionType.LIKES
                                : UserActionType.LIKE;
                        return sendPush(actionType, actorId, postId, EntityType.POST.name(), List.of(ownerId), metadata, true);
                    }));

                    Mono<Void> notifyInteractedPeople = postOwner
                            .defaultIfEmpty("")
                            .flatMap(ownerId -> getPostCommentersExcludingUsers(postId, List.of(ownerId)))
                            .flatMap(recipients -> sendPush(UserActionType.LIKE_OTHER_INTERACT_PEOPLE, actorId, postId, EntityType.POST.name(), recipients, metadata, true));

                    return Mono.when(notifyOwner, notifyInteractedPeople).then();
                }));
    }

    private Mono<Void> handleCommentLike(String actorId, String commentId, String targetOwnerId, String postId) {
        Map<String, String> metadata = baseMetadata(actorId, commentId, EntityType.COMMENT.name());
        metadata.put("COMMENT_ID", commentId);
        metadata.put("POST_ID", postId);

        Mono<String> owner = targetOwnerId.isBlank()
                ? getCommentOwner(commentId)
                : Mono.just(targetOwnerId);

        return enrichActorUsername(actorId, metadata)
                .then(enrichCommentContent(commentId, metadata))
                .then(owner.flatMap(commentOwner ->
                        sendPush(UserActionType.LIKE_COMMENT, actorId, commentId, EntityType.COMMENT.name(), List.of(commentOwner), metadata, true)));
    }

    private Mono<Void> handleStoryLike(
            String actorId,
            String storyId,
            String targetOwnerId,
            String interactionId
    ) {
        if (targetOwnerId == null || targetOwnerId.isBlank()
                || interactionId == null || interactionId.isBlank()) {
            log.warn("|PushModuleNotificationHandler|handleStoryLike|missing data|storyId={}|ownerId={}|interactionId={}",
                    storyId, targetOwnerId, interactionId);
            return Mono.empty();
        }
        Map<String, String> metadata = baseMetadata(actorId, storyId, EntityType.STORY.name());
        metadata.put("STORY_ID", storyId);
        metadata.put("STORY_OWNER_ID", targetOwnerId);
        metadata.put("INTERACTION_ID", interactionId);
        metadata.put("DEDUP_KEY", "LIKE_STORY:" + interactionId);
        return enrichActorUsername(actorId, metadata)
                .then(sendPush(
                        UserActionType.LIKE_STORY,
                        actorId,
                        storyId,
                        EntityType.STORY.name(),
                        List.of(targetOwnerId),
                        metadata,
                        true));
    }

    private Mono<Void> sendPush(
            UserActionType actionType,
            String actorId,
            String entityId,
            String entityType,
            List<String> recipientIds,
            Map<String, String> metadata,
            boolean excludeActor
    ) {
        List<String> recipients = recipientIds == null
                ? List.of()
                : recipientIds.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(recipient -> !recipient.isBlank())
                .filter(recipient -> !excludeActor || !recipient.equals(actorId))
                .distinct()
                .toList();

        if (recipients.isEmpty()) {
            return Mono.empty();
        }

        String postId = resolvePostIdForMuteCheck(entityId, entityType, metadata);
        return filterPostNotificationMutedRecipients(postId, recipients)
                .flatMap(activeRecipients -> {
                    if (activeRecipients.isEmpty()) {
                        return Mono.empty();
                    }

                    return notificationTemplatesRepository.findByActionType(actionType)
                            .switchIfEmpty(Mono.defer(() -> {
                                log.warn("|PushModuleNotificationHandler|sendPush|missing template|actionType={}", actionType);
                                return Mono.empty();
                            }))
                            .flatMap(template -> notificationService.sendNotification(NotificationRequest.builder()
                                .actorId(actorId)
                                .actionType(actionType)
                                .entityId(entityId)
                                .entityType(entityType)
                                .recipientIds(activeRecipients)
                                .title(actionType.name())
                                .content(processTemplate(template, metadata))
                                .metadata(metadata)
                                .dedupKey(metadata.get("DEDUP_KEY"))
                                .notificationType(NotificationType.PUSH)
                                .build()))
                            .then();
                })
                .onErrorResume(error -> {
                    log.error("|PushModuleNotificationHandler|sendPush|failed|actionType={}|entityId={}|error={}",
                            actionType, entityId, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<String> getPostOwner(String postId) {
        return postService.getPostOwnerIdByPostId(postId)
                .onErrorResume(error -> {
                    log.error("|PushModuleNotificationHandler|getPostOwner|failed|postId={}|error={}",
                            postId, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<String> getCommentOwner(String commentId) {
        return commentService.getCommentById(commentId)
                .map(comment -> comment.getUserId() == null ? "" : comment.getUserId())
                .filter(ownerId -> !ownerId.isBlank())
                .onErrorResume(error -> {
                    log.error("|PushModuleNotificationHandler|getCommentOwner|failed|commentId={}|error={}",
                            commentId, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<List<String>> getPostCommenters(String postId) {
        return commentService.getDistinctCommenterUserIdsByPostId(postId)
                .collectList()
                .onErrorResume(error -> {
                    log.error("|PushModuleNotificationHandler|getPostCommenters|failed|postId={}|error={}",
                            postId, error.getMessage());
                    return Mono.just(List.of());
                });
    }

    private Mono<List<String>> getPostCommentersExcludingUsers(String postId, List<String> excludedUserIds) {
        List<String> excluded = excludedUserIds == null
                ? List.of()
                : excludedUserIds.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(userId -> !userId.isBlank())
                .distinct()
                .toList();
        return getPostCommenters(postId)
                .map(commenters -> commenters.stream()
                        .filter(commenterId -> !excluded.contains(commenterId))
                        .toList());
    }

    private Mono<List<String>> filterPostNotificationMutedRecipients(String postId, List<String> recipientIds) {
        if (postId == null || postId.isBlank() || recipientIds == null || recipientIds.isEmpty()) {
            return Mono.just(recipientIds == null ? List.of() : recipientIds);
        }

        return Flux.fromIterable(recipientIds)
                .concatMap(recipientId -> redisTemplate.hasKey(PostNotificationCacheKeys.mutedPostNotification(postId, recipientId))
                        .onErrorResume(error -> {
                            log.error("|PushModuleNotificationHandler|filterPostNotificationMutedRecipients|postId={}|userId={}|error={}",
                                    postId, recipientId, error.getMessage());
                            return Mono.just(false);
                        })
                        .filter(muted -> !Boolean.TRUE.equals(muted))
                        .map(ignored -> recipientId))
                .collectList();
    }

    private String resolvePostIdForMuteCheck(String entityId, String entityType, Map<String, String> metadata) {
        String metadataPostId = metadata == null ? "" : metadata.getOrDefault("POST_ID", "");
        if (metadataPostId != null && !metadataPostId.isBlank()) {
            return metadataPostId;
        }
        return EntityType.POST.name().equals(entityType) ? entityId : "";
    }

    private Mono<Void> enrichActorUsername(String actorId, Map<String, String> metadata) {
        return userIdentityQueryService.resolveUsername(actorId)
                .doOnNext(username -> metadata.put("USERNAME", username))
                .onErrorResume(error -> {
                    log.error("|PushModuleNotificationHandler|enrichActorUsername|failed|actorId={}|error={}",
                            actorId, error.getMessage());
                    metadata.put("USERNAME", actorId);
                    return Mono.empty();
                })
                .then();
    }

    private Mono<Void> enrichPostContent(String postId, Map<String, String> metadata) {
        if (postId == null || postId.isBlank()) {
            metadata.putIfAbsent("CONTENT", "");
            return Mono.empty();
        }

        return postService.getPostById(postId)
                .map(post -> post.getContent() == null ? "" : post.getContent())
                .defaultIfEmpty(metadata.getOrDefault("CONTENT", ""))
                .doOnNext(content -> metadata.put("CONTENT", content))
                .onErrorResume(error -> {
                    log.error("|PushModuleNotificationHandler|enrichPostContent|failed|postId={}|error={}",
                            postId, error.getMessage());
                    metadata.putIfAbsent("CONTENT", "");
                    return Mono.empty();
                })
                .then();
    }

    private Mono<Void> enrichCommentContent(String commentId, Map<String, String> metadata) {
        if (commentId == null || commentId.isBlank()) {
            metadata.putIfAbsent("COMMENT", "");
            return Mono.empty();
        }

        return commentService.getCommentById(commentId)
                .map(comment -> comment.getContent() == null ? "" : comment.getContent())
                .defaultIfEmpty(metadata.getOrDefault("COMMENT", ""))
                .doOnNext(commentContent -> {
                    metadata.put("COMMENT", commentContent);
                    metadata.putIfAbsent("REPLY", commentContent);
                })
                .onErrorResume(error -> {
                    log.error("|PushModuleNotificationHandler|enrichCommentContent|failed|commentId={}|error={}",
                            commentId, error.getMessage());
                    metadata.putIfAbsent("COMMENT", "");
                    return Mono.empty();
                })
                .then();
    }

    private Mono<List<String>> getFollowersOfUser(String userId) {
        return collectFollowers(userId, 0, new ArrayList<>());
    }

    private Mono<List<String>> collectFollowers(String userId, int page, List<String> accumulated) {
        return userFollowerService.getFollowers(userId, page, FOLLOWER_PAGE_SIZE)
                .flatMap(response -> {
                    addFollowerIds(accumulated, response);
                    if (!response.isHasNextPage()) {
                        return Mono.just(accumulated);
                    }
                    return collectFollowers(userId, page + 1, accumulated);
                })
                .onErrorResume(error -> {
                    log.error("|PushModuleNotificationHandler|collectFollowers|failed|userId={}|page={}|error={}",
                            userId, page, error.getMessage());
                    return Mono.just(accumulated);
                });
    }

    private void addFollowerIds(List<String> accumulated, FollowerListResponse response) {
        if (response == null || response.getFollowers() == null) {
            return;
        }

        response.getFollowers().stream()
                .map(FollowerListResponse.FollowerInfo::getUserId)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(userId -> !userId.isBlank())
                .forEach(accumulated::add);
    }

    private String processTemplate(NotificationTemplates template, Map<String, String> metadata) {
        String content = template.getTemplate() == null ? "" : template.getTemplate();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            String value = entry.getValue() == null ? "" : entry.getValue();
            content = content
                    .replace("{{" + entry.getKey() + "}}", value)
                    .replace("{{" + entry.getKey().toLowerCase() + "}}", value);
        }
        return content;
    }

    private Map<String, String> baseMetadata(String actorId, String entityId, String entityType) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("ACTOR_ID", actorId);
        metadata.put("USER_ID", actorId);
        metadata.put("ENTITY_ID", entityId);
        metadata.put("ENTITY_TYPE", entityType);
        return metadata;
    }

    private JsonObject toJsonObject(Object payload) {
        if (payload == null) {
            return new JsonObject();
        }
        if (payload instanceof ConsumerRecord<?, ?> record) {
            return toJsonObject(record.value());
        }
        if (payload instanceof JsonObject jsonObject) {
            return jsonObject;
        }
        if (payload instanceof String rawPayload) {
            return GsonUtils.fromString(rawPayload);
        }
        return GsonUtils.fromObject(payload);
    }

    private String firstString(JsonObject jsonObject, String... fields) {
        for (String field : fields) {
            String value = KafkaUtils.extractString(jsonObject, field);
            if (!value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    private Long firstLong(JsonObject jsonObject, String... fields) {
        for (String field : fields) {
            Long value = KafkaUtils.extractLong(jsonObject, field);
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}

