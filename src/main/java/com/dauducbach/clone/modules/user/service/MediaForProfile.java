package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repositoty.music.MusicsRepository;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.service.PostSseService;
import com.dauducbach.clone.modules.user.dto.request.AvatarUploadRequest;
import com.dauducbach.clone.modules.user.dto.request.MusicSelectRequest;
import com.dauducbach.clone.modules.user.dto.response.ProfileMediaUploadResponse;
import com.dauducbach.clone.modules.user.entity.UserMusics;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.dauducbach.clone.utils.MediaScanUtils;
import com.google.gson.JsonObject;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class MediaForProfile {
    private static final Logger log = LoggerFactory.getLogger(MediaForProfile.class);
    private static final String CHECK_AVATAR_MEDIA_EVENT = "check_avatar_media_event";
    private static final String AVATAR_UPDATE_EVENT = "avatar_update_event";
    private static final String STATUS_PENDING = "PENDING_SCAN";
    private static final String STATUS_APPROVED = "APPROVED";
    private static final String STATUS_REJECTED = "REJECTED";

    UserDetailsRepository userDetailsRepository;
    MusicsRepository musicsRepository;
    MediaService mediaService;
    MediaCompatibilityFacade cloudinaryMediaService;
    PostSseService postSseService;
    KafkaSender<String, String> kafkaSender;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    MediaScanUtils mediaScanUtils;
    UserAuditService userAuditService;

    public Mono<ProfileMediaUploadResponse> uploadAvatar(AvatarUploadRequest request) {
        String userId = normalizeRequired(request.userId(), "userId");
        String avatarUrl = normalizeRequired(request.avatarUrl(), "avatarUrl");
        String publicId = resolvePublicId(avatarUrl);

        log.info("|MediaForProfile|uploadAvatar|userId={}|publicId={}", userId, publicId);
        return ensureUserExists(userId)
                .then(sendAvatarScanEvent(userId, avatarUrl, publicId))
                .thenReturn(new ProfileMediaUploadResponse(userId, OwnerType.AVATAR.name(), userId, STATUS_PENDING, "Avatar is waiting for media validation"))
                .doOnSuccess(response -> log.info("|MediaForProfile|uploadAvatar|pending|userId={}|publicId={}", userId, publicId))
                .doOnError(error -> log.error("|MediaForProfile|uploadAvatar|failed|userId={}|publicId={}|error={}",
                        userId, publicId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.PROFILE_MEDIA_PROCESS_FAILED, "Upload avatar failed", error));
    }

    public Mono<UserMusics> selectProfileMusic(MusicSelectRequest request) {
        String userId = normalizeRequired(request.userId(), "userId");
        String musicDisplayName = normalizeRequired(request.musicDisplayName(), "musicDisplayName");
        String musicSlugName = normalizeRequired(request.musicSlugName(), "musicSlugName");

        log.info("|MediaForProfile|selectProfileMusic|userId={}|musicSlugName={}", userId, musicSlugName);
        return ensureUserExists(userId)
                .then(musicsRepository.findBySlugNameAndDisplayName(musicSlugName, musicDisplayName)
                        .switchIfEmpty(Mono.defer(() -> musicsRepository.findBySlugName(musicSlugName)))
                        .switchIfEmpty(Mono.error(new AppException(
                                ErrorCode.MUSIC_NOT_FOUND,
                                String.format("Music not found for slugName=%s displayName=%s", musicSlugName, musicDisplayName)
                        ))))
                .flatMap(music -> saveUserMusic(userId, music)
                        .flatMap(userMusic -> mediaService.saveFeatureMusic(
                                        userId,
                                        music.getId(),
                                        music.getDisplayName(),
                                        music.getSlugName(),
                                        music.getSongUrl(),
                                        music.getDisplayImages())
                                .then(saveProfileMusicAudit(userId, userMusic, music))
                                .thenReturn(userMusic)))
                .doOnSuccess(userMusic -> log.info("|MediaForProfile|selectProfileMusic|saved|userId={}|musicId={}|userMusicId={}",
                        userId, userMusic.getMusicId(), userMusic.getId()))
                .doOnError(error -> log.error("|MediaForProfile|selectProfileMusic|failed|userId={}|musicSlugName={}|error={}",
                        userId, musicSlugName, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.MUSIC_SAVE_FAILED, "Select profile music failed", error));
    }

    public Mono<PageResponse<Media>> getUploadedMedia(String userId, OwnerType mode, int page, int size) {
        OwnerType ownerType = normalizeProfileOwnerType(mode);
        log.info("|MediaForProfile|getUploadedMedia|userId={}|ownerType={}|page={}|size={}",
                userId, ownerType, page, size);
        return mediaService.getProfileMedia(userId, ownerType, page, size);
    }

    public Mono<Media> getCurrentAvatar(String userId) {
        log.info("|MediaForProfile|getCurrentAvatar|userId={}", userId);
        return mediaService.getCurrentAvatar(userId);
    }

    public Mono<Media> getCurrentAvatar(String userId, MediaDisplayType mediaType) {
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.AVATAR : mediaType;
        return getCurrentAvatar(userId).map(media -> transformMediaForDisplay(media, displayType));
    }

    public Mono<PageResponse<Media>> getProfileMusicHistory(String userId, int page, int size) {
        log.info("|MediaForProfile|getProfileMusicHistory|userId={}|page={}|size={}", userId, page, size);
        return mediaService.getProfileMedia(userId, OwnerType.FEATURE_MUSIC, page, size);
    }

    private Media transformMediaForDisplay(Media media, MediaDisplayType mediaType) {
        media.setUrl(cloudinaryMediaService.transformDeliveryUrl(media.getUrl(), mediaType));
        media.setSecureUrl(cloudinaryMediaService.transformDeliveryUrl(media.getSecureUrl(), mediaType));
        return media;
    }

    @KafkaListener(topics = CHECK_AVATAR_MEDIA_EVENT, groupId = "user-service")
    public CompletableFuture<Void> handleAvatarScanEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(payloadJson, "userId");
        String avatarUrl = KafkaUtils.extractString(payloadJson, "avatarUrl");
        String publicId = KafkaUtils.extractString(payloadJson, "publicId");
        log.info("|MediaForProfile|handleAvatarScanEvent|received|userId={}|publicId={}", userId, publicId);

        if (userId.isBlank() || avatarUrl.isBlank()) {
            log.warn("|MediaForProfile|handleAvatarScanEvent|missing data|userId={}", userId);
            return CompletableFuture.completedFuture(null);
        }

        return mediaScanUtils.scanMedia(avatarUrl, publicId)
                .doOnSuccess(result -> log.info("|MediaForProfile|handleAvatarScanEvent|scan result|userId={}|publicId={}|nsfw={}",
                        userId, publicId, result.nsfw()))
                .flatMap(result -> result.nsfw()
                        ? handleAvatarFailed(userId, avatarUrl, publicId)
                        : handleAvatarSuccess(userId, avatarUrl, publicId))
                .doOnSuccess(v -> log.info("|MediaForProfile|handleAvatarScanEvent|completed|userId={}", userId))
                .doOnError(error -> log.error("|MediaForProfile|handleAvatarScanEvent|failed|userId={}|error={}", userId, error.getMessage()))
                .toFuture();
    }

    private Mono<UserMusics> saveUserMusic(String userId, Musics music) {
        log.info("|MediaForProfile|saveUserMusic|userId={}|musicId={}", userId, music.getId());
        UserMusics userMusic = UserMusics.builder()
                .id(UUID.randomUUID().toString())
                .userId(userId)
                .musicId(music.getId())
                .createdAt(Instant.now())
                .build();
        return r2dbcEntityTemplate.insert(UserMusics.class).using(userMusic)
                .doOnSuccess(saved -> log.info("|MediaForProfile|saveUserMusic|saved|userId={}|musicId={}|userMusicId={}",
                        userId, music.getId(), saved.getId()));
    }

    private Mono<Void> handleAvatarSuccess(String userId, String avatarUrl, String publicId) {
        log.info("|MediaForProfile|handleAvatarSuccess|userId={}|publicId={}", userId, publicId);
        return mediaService.saveCloudinaryMedia(publicId, userId, OwnerType.AVATAR)
                .flatMap(media -> sendAvatarSuccessSse(userId, media)
                        .then(publishAvatarUpdateEvent(userId, avatarUrl, media.getAssetId())))
                .doOnSuccess(v -> log.info("|MediaForProfile|handleAvatarSuccess|completed|userId={}|publicId={}", userId, publicId));
    }

    private Mono<Void> handleAvatarFailed(String userId, String avatarUrl, String publicId) {
        log.warn("|MediaForProfile|handleAvatarFailed|userId={}|publicId={}", userId, publicId);
        return deleteCloudinaryMedia(publicId)
                .then(sendProfileFailureSse(userId, "avatar_upload_event", userId, OwnerType.AVATAR, avatarUrl, "Avatar rejected due to invalid media"))
                .then(saveProfileMediaAudit(userId, AuditActionType.UPLOAD_AVATAR, "AVATAR", userId, "FAILURE", publicId));
    }

    private Mono<Void> saveProfileMediaAudit(String userId,
                                             AuditActionType action,
                                             String resourceType,
                                             String resourceId,
                                             String status,
                                             String publicId) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("publicId", publicId);
        metadata.addProperty("reason", "MEDIA_SCAN_REJECTED");
        return userAuditService.save(AuditLogs.builder()
                .actorId(userId)
                .action(action)
                .resourceType(resourceType)
                .resourceId(resourceId)
                .status(status)
                .metadata(metadata.toString())
                .build());
    }

    private Mono<Void> saveProfileMusicAudit(String userId, UserMusics userMusic, Musics music) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("musicId", music.getId());
        metadata.addProperty("slugName", music.getSlugName());
        metadata.addProperty("displayName", music.getDisplayName());
        return userAuditService.save(AuditLogs.builder()
                .actorId(userId)
                .action(AuditActionType.SELECT_PROFILE_MUSIC)
                .resourceType("FEATURE_MUSIC")
                .resourceId(userMusic.getId())
                .status("SUCCESS")
                .metadata(metadata.toString())
                .build());
    }

    private Mono<Void> sendAvatarScanEvent(String userId, String avatarUrl, String publicId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", userId);
        payload.addProperty("avatarUrl", avatarUrl);
        payload.addProperty("publicId", publicId);
        return sendKafka(CHECK_AVATAR_MEDIA_EVENT, userId, payload);
    }

    private Mono<Void> publishAvatarUpdateEvent(String userId, String avatarUrl, String mediaId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", userId);
        payload.addProperty("avatarUrl", avatarUrl);
        payload.addProperty("mediaId", mediaId);
        return sendKafka(AVATAR_UPDATE_EVENT, userId, payload);
    }

    private Mono<Void> sendKafka(String topic, String key, JsonObject payload) {
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(topic, key, payload.toString()),
                topic
        );
        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|MediaForProfile|sendKafka|topic={}|key={}|error={}", topic, key, error.getMessage()))
                .doOnComplete(() -> log.info("|MediaForProfile|sendKafka|sent|topic={}|key={}", topic, key))
                .then();
    }

    private Mono<Void> sendAvatarSuccessSse(String userId, Media media) {
        JsonObject payload = baseSsePayload(userId, userId, OwnerType.AVATAR, media.getSecureUrl(), STATUS_APPROVED, "Avatar approved");
        payload.addProperty("mediaId", media.getAssetId());
        return postSseService.sendToUser(userId, "avatar_upload_event", payload.toString())
                .doOnSuccess(unused -> log.info("|MediaForProfile|sendAvatarSuccessSse|sent|userId={}|mediaId={}", userId, media.getAssetId()));
    }

    private Mono<Void> sendProfileFailureSse(String userId, String eventName, String entityId, OwnerType ownerType, String mediaUrl, String message) {
        JsonObject payload = baseSsePayload(userId, entityId, ownerType, mediaUrl, STATUS_REJECTED, message);
        return postSseService.sendToUser(userId, eventName, payload.toString())
                .doOnSuccess(unused -> log.info("|MediaForProfile|sendProfileFailureSse|sent|userId={}|eventName={}|entityId={}|ownerType={}",
                        userId, eventName, entityId, ownerType));
    }

    private JsonObject baseSsePayload(String userId, String entityId, OwnerType ownerType, String mediaUrl, String result, String message) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", userId);
        payload.addProperty("entityId", entityId);
        payload.addProperty("ownerType", ownerType.name());
        payload.addProperty("mediaUrl", mediaUrl);
        payload.addProperty("result", result);
        payload.addProperty("message", message);
        return payload;
    }

    private Mono<Void> deleteCloudinaryMedia(String publicId) {
        return cloudinaryMediaService.deleteAsset(publicId);
    }

    private Mono<Void> ensureUserExists(String userId) {
        return userDetailsRepository.existsById(userId)
                .flatMap(exists -> Boolean.TRUE.equals(exists)
                        ? Mono.empty()
                        : Mono.error(new AppException(ErrorCode.USER_DETAILS_NOT_FOUND, String.format("User details not found for userId=%s", userId))));
    }

    private String normalizeRequired(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, fieldName + " is required");
        }
        return value.trim();
    }

    private OwnerType normalizeProfileOwnerType(OwnerType ownerType) {
        if (ownerType == null) {
            throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, "mode is required");
        }
        if (OwnerType.AVATAR.equals(ownerType)
                || OwnerType.POST.equals(ownerType)
                || OwnerType.STORY.equals(ownerType)
                || OwnerType.FEATURE_MUSIC.equals(ownerType)) {
            return ownerType;
        }
        throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, "mode must be AVATAR, POST, STORY or FEATURE_MUSIC");
    }

    private String resolvePublicId(String mediaUrl) {
        String normalized = normalizeRequired(mediaUrl, "mediaUrl");
        int uploadIndex = normalized.indexOf("/upload/");
        if (uploadIndex < 0) {
            return stripExtension(basename(normalized));
        }

        String path = normalized.substring(uploadIndex + "/upload/".length());
        String[] parts = path.split("/");
        int versionIndex = -1;
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].matches("v\\d+")) {
                versionIndex = i;
                break;
            }
        }

        String publicPath;
        if (versionIndex >= 0 && versionIndex < parts.length - 1) {
            publicPath = String.join("/", List.of(parts).subList(versionIndex + 1, parts.length));
        } else {
            publicPath = parts[parts.length - 1];
        }

        int queryIndex = publicPath.indexOf('?');
        if (queryIndex >= 0) {
            publicPath = publicPath.substring(0, queryIndex);
        }
        return stripExtension(publicPath);
    }

    private String basename(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String clean = value;
        int queryIndex = clean.indexOf('?');
        if (queryIndex >= 0) {
            clean = clean.substring(0, queryIndex);
        }
        int slashIndex = Math.max(clean.lastIndexOf('/'), clean.lastIndexOf('\\'));
        return slashIndex >= 0 ? clean.substring(slashIndex + 1) : clean;
    }

    private String stripExtension(String value) {
        int dotIndex = value.lastIndexOf('.');
        return dotIndex > 0 ? value.substring(0, dotIndex) : value;
    }
}
