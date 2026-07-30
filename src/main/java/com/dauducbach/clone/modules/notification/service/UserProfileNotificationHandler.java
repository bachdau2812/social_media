package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.request.NotificationRequest;
import com.dauducbach.clone.modules.notification.entity.NotificationTemplates;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.dauducbach.clone.modules.user.dto.response.FollowerListResponse;
import com.dauducbach.clone.modules.user.service.UserIdentityQueryService;
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
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserProfileNotificationHandler {
    private static final Logger log = LoggerFactory.getLogger(UserProfileNotificationHandler.class);
    private static final int FOLLOWER_PAGE_SIZE = 100;

    NotificationService notificationService;
    NotificationTemplatesRepository notificationTemplatesRepository;
    UserFollowerService userFollowerService;
    UserIdentityQueryService userIdentityQueryService;

    @KafkaListener(topics = "follow_event", groupId = "notification-service")
    public CompletableFuture<Void> handleFollowEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String followerId = KafkaUtils.extractString(payloadJson, "followerId");
        String followingId = KafkaUtils.extractString(payloadJson, "followingId");

        if (followerId.isBlank() || followingId.isBlank()) {
            log.warn("|UserProfileNotificationHandler|handleFollowEvent|missing data|followerId={}|followingId={}", followerId, followingId);
            return CompletableFuture.completedFuture(null);
        }

        Map<String, String> metadata = baseMetadata(followerId, followingId, EntityType.USER.name());
        metadata.put("FOLLOWER_ID", followerId);
        metadata.put("FOLLOWING_ID", followingId);

        return enrichActorUsername(followerId, metadata)
                .then(sendPush(UserActionType.FOLLOW_EVENT, followerId, followingId, EntityType.USER.name(), List.of(followingId), metadata))
                .doOnSuccess(v -> log.info("|UserProfileNotificationHandler|handleFollowEvent|completed|followerId={}|followingId={}", followerId, followingId))
                .doOnError(error -> log.error("|UserProfileNotificationHandler|handleFollowEvent|failed|followerId={}|followingId={}|error={}",
                        followerId, followingId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = "avatar_update_event", groupId = "notification-service")
    public CompletableFuture<Void> handleAvatarUpdateEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(payloadJson, "userId");
        String avatarUrl = KafkaUtils.extractString(payloadJson, "avatarUrl");
        String mediaId = KafkaUtils.extractString(payloadJson, "mediaId");

        if (userId.isBlank()) {
            log.warn("|UserProfileNotificationHandler|handleAvatarUpdateEvent|missing data|userId={}", userId);
            return CompletableFuture.completedFuture(null);
        }

        Map<String, String> metadata = baseMetadata(userId, userId, EntityType.USER.name());
        metadata.put("AVATAR_URL", avatarUrl);
        metadata.put("MEDIA_ID", mediaId);

        return enrichActorUsername(userId, metadata)
                .then(getFollowersOfUser(userId))
                .flatMap(followers -> sendPush(UserActionType.AVATAR_UPDATE, userId, userId, EntityType.USER.name(), followers, metadata))
                .doOnSuccess(v -> log.info("|UserProfileNotificationHandler|handleAvatarUpdateEvent|completed|userId={}", userId))
                .doOnError(error -> log.error("|UserProfileNotificationHandler|handleAvatarUpdateEvent|failed|userId={}|error={}", userId, error.getMessage()))
                .toFuture();
    }

    @KafkaListener(topics = "story_success_event", groupId = "notification-service")
    public CompletableFuture<Void> handleStorySuccessEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String storyId = KafkaUtils.extractString(payloadJson, "storyId");
        String userId = KafkaUtils.extractString(payloadJson, "userId");
        String mediaUrl = KafkaUtils.extractString(payloadJson, "mediaUrl");
        String mediaType = KafkaUtils.extractString(payloadJson, "mediaType");
        String mediaId = KafkaUtils.extractString(payloadJson, "mediaId");
        String publicationId = KafkaUtils.extractString(payloadJson, "publicationId");

        if (storyId.isBlank() || userId.isBlank()) {
            log.warn("|UserProfileNotificationHandler|handleStorySuccessEvent|missing data|storyId={}|userId={}", storyId, userId);
            return CompletableFuture.completedFuture(null);
        }

        Map<String, String> metadata = baseMetadata(userId, storyId, EntityType.STORY.name());
        metadata.put("STORY_ID", storyId);
        metadata.put("MEDIA_URL", mediaUrl);
        metadata.put("MEDIA_TYPE", mediaType);
        metadata.put("MEDIA_ID", mediaId);
        metadata.put("PUBLICATION_ID", publicationId.isBlank() ? storyId : publicationId);
        metadata.put("DEDUP_KEY", "UP_STORY:" + metadata.get("PUBLICATION_ID"));

        return enrichActorUsername(userId, metadata)
                .then(getFollowersOfUser(userId))
                .flatMap(followers -> sendPush(UserActionType.UP_STORY, userId, storyId, EntityType.STORY.name(), followers, metadata))
                .doOnSuccess(v -> log.info("|UserProfileNotificationHandler|handleStorySuccessEvent|completed|storyId={}|userId={}", storyId, userId))
                .doOnError(error -> log.error("|UserProfileNotificationHandler|handleStorySuccessEvent|failed|storyId={}|error={}", storyId, error.getMessage()))
                .toFuture();
    }

    private Mono<Void> sendPush(
            UserActionType actionType,
            String actorId,
            String entityId,
            String entityType,
            List<String> recipientIds,
            Map<String, String> metadata
    ) {
        List<String> recipients = recipientIds == null
                ? List.of()
                : recipientIds.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(recipient -> !recipient.isBlank())
                .filter(recipient -> !recipient.equals(actorId))
                .distinct()
                .toList();

        if (recipients.isEmpty()) {
            return Mono.empty();
        }

        return notificationTemplatesRepository.findByActionType(actionType)
                .switchIfEmpty(Mono.defer(() -> {
                    log.warn("|UserProfileNotificationHandler|sendPush|missing template|actionType={}", actionType);
                    return Mono.empty();
                }))
                .flatMap(template -> notificationService.sendNotification(NotificationRequest.builder()
                        .actorId(actorId)
                        .actionType(actionType)
                        .entityId(entityId)
                        .entityType(entityType)
                        .recipientIds(recipients)
                        .title(actionType.name())
                        .content(processTemplate(template, metadata))
                        .metadata(metadata)
                        .dedupKey(metadata.get("DEDUP_KEY"))
                        .notificationType(NotificationType.PUSH)
                        .build()))
                .then()
                .onErrorResume(error -> {
                    log.error("|UserProfileNotificationHandler|sendPush|failed|actionType={}|entityId={}|error={}",
                            actionType, entityId, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> enrichActorUsername(String actorId, Map<String, String> metadata) {
        return userIdentityQueryService.resolveUsername(actorId)
                .doOnNext(username -> metadata.put("USERNAME", username))
                .onErrorResume(error -> {
                    log.error("|UserProfileNotificationHandler|enrichActorUsername|failed|actorId={}|error={}", actorId, error.getMessage());
                    metadata.put("USERNAME", actorId);
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
                    log.error("|UserProfileNotificationHandler|collectFollowers|failed|userId={}|page={}|error={}",
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
}
