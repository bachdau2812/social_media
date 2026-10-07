package com.dauducbach.clone.modules.user.profile.application;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MediaInspection;
import com.dauducbach.clone.modules.user.dto.request.AvatarUploadRequest;
import com.dauducbach.clone.modules.user.dto.response.ProfileMediaUploadResponse;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
public class AvatarUploadUseCase {
    private static final Logger log = LoggerFactory.getLogger(AvatarUploadUseCase.class);
    private static final String STATUS_PENDING = "PENDING_SCAN";
    private static final String STATUS_APPROVED = "APPROVED";
    private static final String STATUS_REJECTED = "REJECTED";

    private final UserIdentityQuery userIdentityQuery;
    private final AvatarMediaEventPublisher avatarMediaEventPublisher;
    private final UserSsePublisher userSsePublisher;
    private final MediaAssets mediaAssets;
    private final MediaInspection mediaInspection;
    private final AuditRecorder auditRecorder;

    public AvatarUploadUseCase(
            UserIdentityQuery userIdentityQuery,
            AvatarMediaEventPublisher avatarMediaEventPublisher,
            UserSsePublisher userSsePublisher,
            MediaAssets mediaAssets,
            MediaInspection mediaInspection,
            AuditRecorder auditRecorder) {
        this.userIdentityQuery = userIdentityQuery;
        this.avatarMediaEventPublisher = avatarMediaEventPublisher;
        this.userSsePublisher = userSsePublisher;
        this.mediaAssets = mediaAssets;
        this.mediaInspection = mediaInspection;
        this.auditRecorder = auditRecorder;
    }

    public Mono<ProfileMediaUploadResponse> uploadAvatar(AvatarUploadRequest request) {
        String userId = normalizeRequired(request.userId(), "userId");
        String avatarUrl = normalizeRequired(request.avatarUrl(), "avatarUrl");
        String publicId = resolvePublicId(avatarUrl);

        log.info("|AvatarUploadUseCase|uploadAvatar|userId={}|publicId={}", userId, publicId);
        return ensureUserExists(userId)
                .then(avatarMediaEventPublisher.requestAvatarScan(userId, avatarUrl, publicId))
                .thenReturn(new ProfileMediaUploadResponse(
                        userId,
                        OwnerType.AVATAR.name(),
                        userId,
                        STATUS_PENDING,
                        "Avatar is waiting for media validation"))
                .doOnError(error -> log.error("|AvatarUploadUseCase|uploadAvatar|failed|userId={}|publicId={}|error={}",
                        userId, publicId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.PROFILE_MEDIA_PROCESS_FAILED, "Upload avatar failed", error));
    }

    public Mono<Void> reviewAvatar(AvatarMediaReviewRequested request) {
        if (request == null || !hasText(request.userId()) || !hasText(request.avatarUrl())) {
            log.warn("|AvatarUploadUseCase|reviewAvatar|missing data");
            return Mono.empty();
        }

        String userId = request.userId().trim();
        String avatarUrl = request.avatarUrl().trim();
        String publicId = request.publicId() == null ? "" : request.publicId().trim();
        return mediaInspection.inspect(avatarUrl, publicId)
                .flatMap(result -> result.nsfw()
                        ? rejectAvatar(userId, avatarUrl, publicId)
                        : approveAvatar(userId, avatarUrl, publicId))
                .doOnError(error -> log.error("|AvatarUploadUseCase|reviewAvatar|failed|userId={}|error={}",
                        userId, error.getMessage()));
    }

    private Mono<Void> approveAvatar(String userId, String avatarUrl, String publicId) {
        return mediaAssets.registerCloudinaryAsset(publicId, userId, OwnerType.AVATAR)
                .flatMap(media -> avatarMediaEventPublisher
                        .publishAvatarUpdated(userId, media.secureUrl(), media.assetId())
                        .then(Mono.defer(() -> sendAvatarSuccessSse(userId, media, publicId))));
    }

    private Mono<Void> rejectAvatar(String userId, String avatarUrl, String publicId) {
        return mediaAssets.deleteAsset(publicId)
                .then(sendProfileFailureSse(
                        userId,
                        "avatar_upload_event",
                        userId,
                        OwnerType.AVATAR,
                        avatarUrl,
                        publicId,
                        "Avatar rejected due to invalid media"))
                .then(saveProfileMediaAudit(userId, publicId));
    }

    private Mono<Void> sendAvatarSuccessSse(String userId, MediaAssetView media, String publicId) {
        JsonObject payload = baseSsePayload(
                userId, userId, OwnerType.AVATAR, media.secureUrl(), STATUS_APPROVED, "Avatar approved");
        payload.addProperty("mediaId", media.assetId());
        payload.addProperty("publicId", publicId);
        return userSsePublisher.sendToUser(userId, "avatar_upload_event", payload.toString());
    }

    private Mono<Void> sendProfileFailureSse(
            String userId,
            String eventName,
            String entityId,
            OwnerType ownerType,
            String mediaUrl,
            String publicId,
            String message) {
        JsonObject payload = baseSsePayload(userId, entityId, ownerType, mediaUrl, STATUS_REJECTED, message);
        payload.addProperty("publicId", publicId);
        return userSsePublisher.sendToUser(userId, eventName, payload.toString());
    }

    private Mono<Void> saveProfileMediaAudit(String userId, String publicId) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("publicId", publicId);
        metadata.addProperty("reason", "MEDIA_SCAN_REJECTED");
        return auditRecorder.record(new AuditEntry(
                userId,
                AuditActionType.UPLOAD_AVATAR,
                "AVATAR",
                userId,
                "FAILURE",
                metadata.toString(),
                null));
    }

    private JsonObject baseSsePayload(
            String userId,
            String entityId,
            OwnerType ownerType,
            String mediaUrl,
            String result,
            String message) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", userId);
        payload.addProperty("entityId", entityId);
        payload.addProperty("ownerType", ownerType.name());
        payload.addProperty("mediaUrl", mediaUrl);
        payload.addProperty("result", result);
        payload.addProperty("message", message);
        return payload;
    }

    private Mono<Void> ensureUserExists(String userId) {
        return userIdentityQuery.exists(userId)
                .flatMap(exists -> Boolean.TRUE.equals(exists)
                        ? Mono.empty()
                        : Mono.error(new AppException(
                                ErrorCode.USER_DETAILS_NOT_FOUND,
                                String.format("User details not found for userId=%s", userId))));
    }

    private String resolvePublicId(String mediaUrl) {
        int uploadIndex = mediaUrl.indexOf("/upload/");
        if (uploadIndex < 0) {
            return stripExtension(basename(mediaUrl));
        }

        String path = mediaUrl.substring(uploadIndex + "/upload/".length());
        String[] parts = path.split("/");
        int versionIndex = -1;
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].matches("v\\d+")) {
                versionIndex = i;
                break;
            }
        }

        String publicPath = versionIndex >= 0 && versionIndex < parts.length - 1
                ? String.join("/", List.of(parts).subList(versionIndex + 1, parts.length))
                : parts[parts.length - 1];
        int queryIndex = publicPath.indexOf('?');
        return stripExtension(queryIndex >= 0 ? publicPath.substring(0, queryIndex) : publicPath);
    }

    private String basename(String value) {
        int queryIndex = value.indexOf('?');
        String clean = queryIndex >= 0 ? value.substring(0, queryIndex) : value;
        int slashIndex = Math.max(clean.lastIndexOf('/'), clean.lastIndexOf('\\'));
        return slashIndex >= 0 ? clean.substring(slashIndex + 1) : clean;
    }

    private String stripExtension(String value) {
        int dotIndex = value.lastIndexOf('.');
        return dotIndex > 0 ? value.substring(0, dotIndex) : value;
    }

    private String normalizeRequired(String value, String fieldName) {
        if (!hasText(value)) {
            throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, fieldName + " is required");
        }
        return value.trim();
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
