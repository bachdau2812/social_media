package com.dauducbach.clone.modules.audit.service;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import org.springframework.dao.DuplicateKeyException;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.repository.AuditLogsRepository;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.dauducbach.clone.commons.serialization.JsonPayloadReader;
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
import java.util.concurrent.CompletableFuture;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserAuditService implements AuditRecorder {
    private static final Logger log = LoggerFactory.getLogger(UserAuditService.class);
    private static final String STATUS_SUCCESS = "SUCCESS";
    private static final String ACTOR_TYPE_USER = "USER";
    private static final String ACTOR_TYPE_EMAIL = "EMAIL";
    private static final String ACTOR_TYPE_UNKNOWN = "UNKNOWN";
    private static final String RESOURCE_PASSWORD = "PASSWORD";
    private static final String RESOURCE_AVATAR = "AVATAR";
    private static final String RESOURCE_STORY = "STORY";

    AuditLogsRepository auditLogsRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;

    @Override
    public Mono<Void> recordRequiredInteraction(AuditEntry entry) {
        return Mono.defer(() -> {
            validateRequiredInteraction(entry);
            AuditLogs audit = prepareAuditLog(toAuditLog(entry));
            JsonObject metadata = GsonUtils.fromString(audit.getMetadata());
            return r2dbcEntityTemplate.insert(AuditLogs.class).using(audit)
                    .onErrorResume(DuplicateKeyException.class, error -> auditLogsRepository
                            .findBySourceEventId(entry.sourceEventId())
                            .filter(existing -> entry.actorId().equals(existing.getActorId())
                                    && entry.resourceId().equals(existing.getResourceId())
                                    && entry.action() == existing.getAction()
                                    && STATUS_SUCCESS.equals(existing.getStatus())
                                    && metadata.equals(GsonUtils.fromString(existing.getMetadata())))
                            .switchIfEmpty(Mono.error(error)))
                    .then();
        });
    }

    @Override
    public Mono<Void> record(AuditEntry entry) {
        if (entry == null || entry.action() == null) {
            log.warn("|UserAuditService|record|skip invalid audit entry");
            return Mono.empty();
        }

        AuditLogs prepared = prepareAuditLog(toAuditLog(entry));
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
        return record(new AuditEntry(normalizeActorId(actorId), resolveActorType(actorId), action, resourceType, resourceId, status,
                metadata == null ? null : metadata.toString(), null));
    }

    @KafkaListener(topics = "profile_creation_event", groupId = "audit-service")
    public CompletableFuture<Void> handleProfileCreationEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = JsonPayloadReader.extractString(json, "userId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("username", JsonPayloadReader.extractString(json, "username"));
        metadata.addProperty("email", JsonPayloadReader.extractString(json, "email"));

        return record(AuditActionType.REGISTER, userId, EntityType.USER.name(), userId, STATUS_SUCCESS, metadata).toFuture();
    }




    @KafkaListener(topics = "follow_event", groupId = "audit-service")
    public CompletableFuture<Void> handleFollowEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String followerId = JsonPayloadReader.extractString(json, "followerId");
        String followingId = JsonPayloadReader.extractString(json, "followingId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("followingId", followingId);

        return record(AuditActionType.FOLLOW, followerId, EntityType.USER.name(), followingId, STATUS_SUCCESS, metadata).toFuture();
    }

    @KafkaListener(topics = "un_follow_event", groupId = "audit-service")
    public CompletableFuture<Void> handleUnfollowEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String followerId = JsonPayloadReader.extractString(json, "followerId");
        String followingId = JsonPayloadReader.extractString(json, "followingId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("followingId", followingId);

        return record(AuditActionType.UNFOLLOW, followerId, EntityType.USER.name(), followingId, STATUS_SUCCESS, metadata).toFuture();
    }

    @KafkaListener(topics = "avatar_update_event", groupId = "audit-service")
    public CompletableFuture<Void> handleAvatarUpdateEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = JsonPayloadReader.extractString(json, "userId");
        String mediaId = JsonPayloadReader.extractString(json, "mediaId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("mediaId", mediaId);

        return record(AuditActionType.UPLOAD_AVATAR, userId, RESOURCE_AVATAR, mediaId, STATUS_SUCCESS, metadata).toFuture();
    }

    @KafkaListener(topics = "story_success_event", groupId = "audit-service")
    public CompletableFuture<Void> handleStorySuccessEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String userId = JsonPayloadReader.extractString(json, "userId");
        String storyId = JsonPayloadReader.extractString(json, "storyId");
        JsonObject metadata = new JsonObject();
        metadata.addProperty("mediaId", JsonPayloadReader.extractString(json, "mediaId"));
        metadata.addProperty("mediaType", JsonPayloadReader.extractString(json, "mediaType"));
        metadata.addProperty("hasMusic", !JsonPayloadReader.extractString(json, "musicUrl").isBlank());

        return record(AuditActionType.UPLOAD_STORY, userId, RESOURCE_STORY, storyId, STATUS_SUCCESS, metadata).toFuture();
    }

    @KafkaListener(topics = "forget_password_event", groupId = "audit-service")
    public CompletableFuture<Void> handleForgetPasswordEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String email = JsonPayloadReader.extractString(json, "email");
        return record(AuditActionType.FORGET_PASSWORD, email, RESOURCE_PASSWORD, email, STATUS_SUCCESS, emailMetadata(email)).toFuture();
    }

    @KafkaListener(topics = "new_password_event", groupId = "audit-service")
    public CompletableFuture<Void> handleNewPasswordEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String email = JsonPayloadReader.extractString(json, "email");
        return record(AuditActionType.RESET_PASSWORD, email, RESOURCE_PASSWORD, email, STATUS_SUCCESS, emailMetadata(email)).toFuture();
    }

    @KafkaListener(topics = "new_password_and_username_event", groupId = "audit-service")
    public CompletableFuture<Void> handleNewPasswordAndUsernameEvent(@Payload String payload) {
        JsonObject json = GsonUtils.fromString(payload);
        String email = JsonPayloadReader.extractString(json, "email");
        return record(AuditActionType.RESET_PASSWORD, email, RESOURCE_PASSWORD, email, STATUS_SUCCESS, emailMetadata(email)).toFuture();
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

    private AuditLogs toAuditLog(AuditEntry entry) {
        return AuditLogs.builder()
                .actorId(entry.actorId())
                .actorType(entry.actorType() == null ? ACTOR_TYPE_USER : entry.actorType())
                .action(entry.action())
                .resourceType(entry.resourceType())
                .resourceId(entry.resourceId())
                .status(entry.status())
                .metadata(entry.metadataJson())
                .sourceEventId(entry.sourceEventId())
                .build();
    }

    private void validateRequiredInteraction(AuditEntry entry) {
        if (entry == null || entry.actorId() == null || entry.actorId().isBlank()
                || entry.resourceId() == null || entry.resourceId().isBlank()
                || entry.sourceEventId() == null || entry.sourceEventId().isBlank()) {
            throw new IllegalArgumentException("Required post interaction is missing its stable identity");
        }
        if (entry.action() != AuditActionType.LIKE_POST && entry.action() != AuditActionType.COMMENT_POST
                && entry.action() != AuditActionType.REPOST_POST) {
            throw new IllegalArgumentException("Unsupported required post interaction action");
        }
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
