package com.dauducbach.clone.modules.audit.service;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.repositoty.AuditLogsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserAuditService {
    private static final Logger log = LoggerFactory.getLogger(UserAuditService.class);
    private static final String STATUS_SUCCESS = "SUCCESS";
    private static final String STATUS_FAILURE = "FAILURE";
    private static final String ACTOR_TYPE_USER = "USER";
    private static final String ACTOR_TYPE_EMAIL = "EMAIL";
    private static final String ACTOR_TYPE_UNKNOWN = "UNKNOWN";
    private static final String RESOURCE_AUTH = "AUTH";
    private static final String RESOURCE_PASSWORD = "PASSWORD";
    private static final String RESOURCE_AVATAR = "AVATAR";
    private static final String RESOURCE_STORY = "STORY";

    AuditLogsRepository auditLogsRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;

    public Mono<Void> save(AuditLogs auditLog) {
        if (auditLog == null || auditLog.getAction() == null) {
            log.warn("|UserAuditService|save|skip invalid audit log");
            return Mono.empty();
        }

        AuditLogs prepared = prepareAuditLog(auditLog);
        return r2dbcEntityTemplate.insert(AuditLogs.class).using(prepared)
                .doOnSuccess(saved -> log.info("|UserAuditService|save|saved|auditId={}|actorId={}|action={}|status={}",
                        saved.getId(), saved.getActorId(), saved.getAction(), saved.getStatus()))
                .doOnError(error -> log.error("|UserAuditService|save|failed|actorId={}|action={}|error={}",
                        prepared.getActorId(), prepared.getAction(), error.getMessage()))
                .onErrorResume(error -> Mono.empty())
                .then();
    }

    public Mono<Void> record(AuditActionType action,
                             String actorId,
                             String resourceType,
                             String resourceId,
                             String status,
                             JsonObject metadata) {
        return save(AuditLogs.builder()
                .actorId(normalizeActorId(actorId))
                .actorType(resolveActorType(actorId))
                .action(action)
                .resourceType(resourceType)
                .resourceId(resourceId)
                .status(status)
                .metadata(metadata == null ? null : metadata.toString())
                .build());
    }

    @KafkaListener(topics = "profile_creation_event", groupId = "audit-service")
    public void handleProfileCreationEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(json, "userId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("username", KafkaUtils.extractString(json, "username"));
        metadata.addProperty("email", KafkaUtils.extractString(json, "email"));

        record(AuditActionType.REGISTER, userId, EntityType.USER.name(), userId, STATUS_SUCCESS, metadata).subscribe();
    }

    @KafkaListener(topics = "post_upload_event", groupId = "audit-service")
    public void handlePostUploadEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String postId = KafkaUtils.extractString(json, "post_id");
        String userId = KafkaUtils.extractString(json, "userId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("contentLength", KafkaUtils.extractString(json, "content").length());

        record(AuditActionType.CREATE_POST, userId, EntityType.POST.name(), postId, STATUS_SUCCESS, metadata).subscribe();
    }

    @KafkaListener(topics = "comment_success_event", groupId = "audit-service")
    public void handleCommentSuccessEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String commentId = KafkaUtils.extractString(json, "commentId");
        String userId = KafkaUtils.extractString(json, "userId");
        String postId = KafkaUtils.extractString(json, "postId");
        String parentId = KafkaUtils.extractString(json, "parentId");

        JsonObject metadata = new JsonObject();
        metadata.addProperty("postId", postId);
        metadata.addProperty("parentId", parentId);
        metadata.addProperty("hasMedia", !KafkaUtils.extractString(json, "mediaUrl").isBlank());

        AuditActionType action = parentId.isBlank()
                ? AuditActionType.COMMENT_POST
                : AuditActionType.COMMENT_REPLY;
        record(action, userId, EntityType.COMMENT.name(), commentId, STATUS_SUCCESS, metadata).subscribe();
    }

    @KafkaListener(topics = "like_event", groupId = "audit-service")
    public void handleLikeEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String actorId = KafkaUtils.extractString(json, "actorId");
        String targetId = KafkaUtils.extractString(json, "targetId");
        String targetType = KafkaUtils.extractString(json, "targetType").toUpperCase();

        JsonObject metadata = new JsonObject();
        metadata.addProperty("targetOwnerId", KafkaUtils.extractString(json, "targetOwnerId"));
        metadata.addProperty("postId", KafkaUtils.extractString(json, "postId"));
        metadata.addProperty("parentCommentId", KafkaUtils.extractString(json, "parentCommentId"));
        metadata.addProperty("likeCount", KafkaUtils.extractLong(json, "likeCount"));

        AuditActionType action = EntityType.COMMENT.name().equals(targetType)
                ? AuditActionType.LIKE_COMMENT
                : AuditActionType.LIKE_POST;
        record(action, actorId, targetType, targetId, STATUS_SUCCESS, metadata).subscribe();
    }

    @KafkaListener(topics = "follow_event", groupId = "audit-service")
    public void handleFollowEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String followerId = KafkaUtils.extractString(json, "followerId");
        String followingId = KafkaUtils.extractString(json, "followingId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("followingId", followingId);

        record(AuditActionType.FOLLOW, followerId, EntityType.USER.name(), followingId, STATUS_SUCCESS, metadata).subscribe();
    }

    @KafkaListener(topics = "un_follow_event", groupId = "audit-service")
    public void handleUnfollowEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String followerId = KafkaUtils.extractString(json, "followerId");
        String followingId = KafkaUtils.extractString(json, "followingId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("followingId", followingId);

        record(AuditActionType.UNFOLLOW, followerId, EntityType.USER.name(), followingId, STATUS_SUCCESS, metadata).subscribe();
    }

    @KafkaListener(topics = "avatar_update_event", groupId = "audit-service")
    public void handleAvatarUpdateEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(json, "userId");
        String mediaId = KafkaUtils.extractString(json, "mediaId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("mediaId", mediaId);

        record(AuditActionType.UPLOAD_AVATAR, userId, RESOURCE_AVATAR, mediaId, STATUS_SUCCESS, metadata).subscribe();
    }

    @KafkaListener(topics = "story_success_event", groupId = "audit-service")
    public void handleStorySuccessEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(json, "userId");
        String storyId = KafkaUtils.extractString(json, "storyId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("mediaId", KafkaUtils.extractString(json, "mediaId"));
        metadata.addProperty("mediaType", KafkaUtils.extractString(json, "mediaType"));
        metadata.addProperty("hasMusic", !KafkaUtils.extractString(json, "musicUrl").isBlank());

        record(AuditActionType.UPLOAD_STORY, userId, RESOURCE_STORY, storyId, STATUS_SUCCESS, metadata).subscribe();
    }

    @KafkaListener(topics = "forget_password_event", groupId = "audit-service")
    public void handleForgetPasswordEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String email = KafkaUtils.extractString(json, "email");
        record(AuditActionType.FORGET_PASSWORD, email, RESOURCE_PASSWORD, email, STATUS_SUCCESS, emailMetadata(email)).subscribe();
    }

    @KafkaListener(topics = "new_password_event", groupId = "audit-service")
    public void handleNewPasswordEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String email = KafkaUtils.extractString(json, "email");
        record(AuditActionType.RESET_PASSWORD, email, RESOURCE_PASSWORD, email, STATUS_SUCCESS, emailMetadata(email)).subscribe();
    }

    @KafkaListener(topics = "new_password_and_username_event", groupId = "audit-service")
    public void handleNewPasswordAndUsernameEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String email = KafkaUtils.extractString(json, "email");
        record(AuditActionType.RESET_PASSWORD, email, RESOURCE_PASSWORD, email, STATUS_SUCCESS, emailMetadata(email)).subscribe();
    }

    private AuditLogs prepareAuditLog(AuditLogs auditLog) {
        if (auditLog.getId() == null || auditLog.getId().isBlank()) {
            auditLog.setId(UUID.randomUUID().toString());
        }
        auditLog.setActorId(normalizeActorId(auditLog.getActorId()));
        if (auditLog.getActorType() == null || auditLog.getActorType().isBlank()) {
            auditLog.setActorType(resolveActorType(auditLog.getActorId()));
        }
        if (auditLog.getStatus() == null || auditLog.getStatus().isBlank()) {
            auditLog.setStatus(STATUS_SUCCESS);
        }
        if (auditLog.getCreatedAt() == null) {
            auditLog.setCreatedAt(Instant.now());
        }
        return auditLog;
    }

    private JsonObject emailMetadata(String email) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("email", email);
        return metadata;
    }

    private String normalizeActorId(String actorId) {
        return actorId == null || actorId.isBlank() ? ACTOR_TYPE_UNKNOWN : actorId;
    }

    private String resolveActorType(String actorId) {
        if (actorId == null || actorId.isBlank() || ACTOR_TYPE_UNKNOWN.equals(actorId)) {
            return ACTOR_TYPE_UNKNOWN;
        }
        return actorId.contains("@") ? ACTOR_TYPE_EMAIL : ACTOR_TYPE_USER;
    }
}
