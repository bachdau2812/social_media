package com.dauducbach.clone.modules.user.service;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.constant.OwnerType;
import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.modules.post.entity.Media;
import com.dauducbach.clone.modules.post.service.MediaService;
import com.dauducbach.clone.modules.post.service.PostSseService;
import com.dauducbach.clone.modules.user.dto.request.AvatarUploadRequest;
import com.dauducbach.clone.modules.user.dto.request.MusicSelectRequest;
import com.dauducbach.clone.modules.user.dto.request.StoryCreateRequest;
import com.dauducbach.clone.modules.user.dto.response.ProfileMediaUploadResponse;
import com.dauducbach.clone.modules.user.entity.Musics;
import com.dauducbach.clone.modules.user.entity.UserMusics;
import com.dauducbach.clone.modules.user.entity.UserStories;
import com.dauducbach.clone.modules.user.repositoty.MusicsRepository;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.user.repositoty.UserStoriesRepository;
import com.dauducbach.clone.utils.CloudinaryUtils;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.google.gson.JsonObject;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class MediaForProfile {
    private static final Logger log = LoggerFactory.getLogger(MediaForProfile.class);
    private static final String CHECK_AVATAR_MEDIA_EVENT = "check_avatar_media_event";
    private static final String CHECK_STORY_MEDIA_EVENT = "check_story_media_event";
    private static final String AVATAR_UPDATE_EVENT = "avatar_update_event";
    private static final String STORY_SUCCESS_EVENT = "story_success_event";
    private static final String STATUS_PENDING = "PENDING_SCAN";
    private static final String STATUS_APPROVED = "APPROVED";
    private static final String STATUS_REJECTED = "REJECTED";
    private static final Duration STORY_TTL = Duration.ofHours(24);

    UserDetailsRepository userDetailsRepository;
    MusicsRepository musicsRepository;
    UserStoriesRepository userStoriesRepository;
    MediaService mediaService;
    PostSseService postSseService;
    KafkaSender<String, String> kafkaSender;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    WebClient webClient;
    Cloudinary cloudinary;

    @NonFinal
    @Value("${post.media.scan.api-url:http://localhost:8000/api/v1/scan}")
    String scanApiUrl;

    public Mono<ProfileMediaUploadResponse> uploadAvatar(AvatarUploadRequest request) {
        String userId = normalizeRequired(request.userId(), "userId");
        String avatarUrl = normalizeRequired(request.avatarUrl(), "avatarUrl");
        String publicId = resolvePublicId(avatarUrl);

        return ensureUserExists(userId)
                .then(sendAvatarScanEvent(userId, avatarUrl, publicId))
                .thenReturn(new ProfileMediaUploadResponse(userId, OwnerType.AVATAR.name(), userId, STATUS_PENDING, "Avatar is waiting for media validation"))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.PROFILE_MEDIA_PROCESS_FAILED, "Upload avatar failed", error));
    }

    public Mono<ProfileMediaUploadResponse> createStory(StoryCreateRequest request) {
        String userId = normalizeRequired(request.userId(), "userId");
        String mediaUrl = normalizeRequired(request.mediaUrl(), "mediaUrl");
        String musicUrl = normalizeOptional(request.musicUrl());
        Long musicStart = request.musicStart();
        Long musicEnd = request.musicEnd();
        validateStoryMusicSegment(musicUrl, musicStart, musicEnd);
        String publicId = resolvePublicId(mediaUrl);
        String storyId = UUID.randomUUID().toString();
        Instant now = Instant.now();

        UserStories story = UserStories.builder()
                .id(storyId)
                .userId(userId)
                .mediaUrl(mediaUrl)
                .mediaType(resolveMediaType(mediaUrl))
                .musicUrl(musicUrl)
                .musicStart(musicStart)
                .musicEnd(musicEnd)
                .status(STATUS_PENDING)
                .createdAt(now)
                .expiredAt(now.plus(STORY_TTL))
                .build();

        return ensureUserExists(userId)
                .then(r2dbcEntityTemplate.insert(UserStories.class).using(story))
                .flatMap(saved -> sendStoryScanEvent(saved, publicId).thenReturn(saved))
                .map(saved -> new ProfileMediaUploadResponse(userId, OwnerType.STORY.name(), saved.getId(), STATUS_PENDING, "Story is waiting for media validation"))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.STORY_SAVE_FAILED, "Create story failed", error));
    }

    public Mono<UserMusics> selectProfileMusic(MusicSelectRequest request) {
        String userId = normalizeRequired(request.userId(), "userId");
        String musicDisplayName = normalizeRequired(request.musicDisplayName(), "musicDisplayName");
        String musicSlugName = normalizeRequired(request.musicSlugName(), "musicSlugName");

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
                                .thenReturn(userMusic)))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.MUSIC_SAVE_FAILED, "Select profile music failed", error));
    }

    public Mono<PageResponse<Media>> getUploadedMedia(String userId, OwnerType mode, int page, int size) {
        OwnerType ownerType = normalizeProfileOwnerType(mode);
        return mediaService.getProfileMedia(userId, ownerType, page, size);
    }

    public Mono<Media> getCurrentAvatar(String userId) {
        return mediaService.getCurrentAvatar(userId);
    }

    public Mono<PageResponse<Media>> getProfileMusicHistory(String userId, int page, int size) {
        return mediaService.getProfileMedia(userId, OwnerType.FEATURE_MUSIC, page, size);
    }

    public Mono<PageResponse<UserStories>> getStories(String userId, int page, int size) {
        if (userId == null || userId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "userId is required"));
        }

        int pageNumber = Math.max(page, 0);
        int pageSize = Math.clamp(size, 1, 100);
        return userStoriesRepository.countByUserId(userId)
                .flatMap(total -> userStoriesRepository.findByUserIdOrderByCreatedAtDesc(userId, PageRequest.of(pageNumber, pageSize))
                        .collectList()
                        .map(stories -> PageResponse.of(stories, pageNumber, total, pageSize)))
                .onErrorMap(error -> new AppException(
                        ErrorCode.STORY_SAVE_FAILED,
                        String.format("Fetch stories failed for userId=%s", userId),
                        error
                ));
    }

    @KafkaListener(topics = CHECK_AVATAR_MEDIA_EVENT, groupId = "user-service")
    public void handleAvatarScanEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String userId = KafkaUtils.extractString(payloadJson, "userId");
        String avatarUrl = KafkaUtils.extractString(payloadJson, "avatarUrl");
        String publicId = KafkaUtils.extractString(payloadJson, "publicId");

        if (userId.isBlank() || avatarUrl.isBlank()) {
            log.warn("|MediaForProfile|handleAvatarScanEvent|missing data|userId={}", userId);
            return;
        }

        MediaUploadRequest media = MediaUploadRequest.builder()
                .secureUrl(avatarUrl)
                .publicId(publicId)
                .build();

        scanSingleMedia(media)
                .flatMap(result -> result.nsfw()
                        ? handleAvatarFailed(userId, avatarUrl, publicId)
                        : handleAvatarSuccess(userId, avatarUrl, publicId))
                .doOnSuccess(v -> log.info("|MediaForProfile|handleAvatarScanEvent|completed|userId={}", userId))
                .doOnError(error -> log.error("|MediaForProfile|handleAvatarScanEvent|failed|userId={}|error={}", userId, error.getMessage()))
                .subscribe();
    }

    @KafkaListener(topics = CHECK_STORY_MEDIA_EVENT, groupId = "user-service")
    public void handleStoryScanEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String storyId = KafkaUtils.extractString(payloadJson, "storyId");
        String userId = KafkaUtils.extractString(payloadJson, "userId");
        String mediaUrl = KafkaUtils.extractString(payloadJson, "mediaUrl");
        String publicId = KafkaUtils.extractString(payloadJson, "publicId");

        if (storyId.isBlank() || userId.isBlank() || mediaUrl.isBlank()) {
            log.warn("|MediaForProfile|handleStoryScanEvent|missing data|storyId={}|userId={}", storyId, userId);
            return;
        }

        MediaUploadRequest media = MediaUploadRequest.builder()
                .secureUrl(mediaUrl)
                .publicId(publicId)
                .build();

        scanSingleMedia(media)
                .flatMap(result -> result.nsfw()
                        ? handleStoryFailed(storyId, userId, mediaUrl, publicId)
                        : handleStorySuccess(storyId, userId, mediaUrl, publicId))
                .doOnSuccess(v -> log.info("|MediaForProfile|handleStoryScanEvent|completed|storyId={}", storyId))
                .doOnError(error -> log.error("|MediaForProfile|handleStoryScanEvent|failed|storyId={}|error={}", storyId, error.getMessage()))
                .subscribe();
    }

    private Mono<UserMusics> saveUserMusic(String userId, Musics music) {
        UserMusics userMusic = UserMusics.builder()
                .id(UUID.randomUUID().toString())
                .userId(userId)
                .musicId(music.getId())
                .createdAt(Instant.now())
                .build();
        return r2dbcEntityTemplate.insert(UserMusics.class).using(userMusic);
    }

    private Mono<Void> handleAvatarSuccess(String userId, String avatarUrl, String publicId) {
        return mediaService.saveCloudinaryMedia(publicId, userId, OwnerType.AVATAR)
                .flatMap(media -> sendAvatarSuccessSse(userId, media)
                        .then(publishAvatarUpdateEvent(userId, avatarUrl, media.getAssetId())));
    }

    private Mono<Void> handleAvatarFailed(String userId, String avatarUrl, String publicId) {
        return deleteCloudinaryMedia(publicId)
                .then(sendProfileFailureSse(userId, "avatar_upload_event", userId, OwnerType.AVATAR, avatarUrl, "Avatar rejected due to invalid media"));
    }

    private Mono<Void> handleStorySuccess(String storyId, String userId, String mediaUrl, String publicId) {
        return userStoriesRepository.findById(storyId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.STORY_NOT_FOUND, "Story not found")))
                .flatMap(story -> mediaService.saveCloudinaryMedia(publicId, userId, OwnerType.STORY)
                        .flatMap(media -> {
                            story.setStatus(STATUS_APPROVED);
                            story.setMediaType(resolveMediaTypeFromMedia(media, mediaUrl));
                            return userStoriesRepository.save(story)
                                    .flatMap(saved -> sendStorySuccessSse(userId, saved, media)
                                            .then(publishStorySuccessEvent(saved, media.getAssetId())));
                        }));
    }

    private Mono<Void> handleStoryFailed(String storyId, String userId, String mediaUrl, String publicId) {
        return userStoriesRepository.findById(storyId)
                .flatMap(story -> {
                    story.setStatus(STATUS_REJECTED);
                    return userStoriesRepository.save(story);
                })
                .then(deleteCloudinaryMedia(publicId))
                .then(sendProfileFailureSse(userId, "story_upload_event", storyId, OwnerType.STORY, mediaUrl, "Story rejected due to invalid media"));
    }

    private Mono<ScanResult> scanSingleMedia(MediaUploadRequest item) {
        if (item.getSecureUrl() == null || item.getSecureUrl().isBlank()) {
            return Mono.just(new ScanResult(true));
        }

        return webClient.get()
                .uri(item.getSecureUrl())
                .retrieve()
                .bodyToMono(byte[].class)
                .flatMap(bytes -> callScanApi(bytes, item.getPublicId(), item.getSecureUrl()))
                .onErrorResume(error -> {
                    log.error("|MediaForProfile|scanSingleMedia|failed|url={}|error={}", item.getSecureUrl(), error.getMessage());
                    return Mono.just(new ScanResult(true));
                });
    }

    private Mono<ScanResult> callScanApi(byte[] bytes, String publicId, String secureUrl) {
        MultiValueMap<String, Object> formData = new LinkedMultiValueMap<>();
        formData.add("file", new ByteArrayResource(bytes) {
            @Override
            public String getFilename() {
                return buildScanFilename(publicId, secureUrl);
            }
        });

        return webClient.post()
                .uri(scanApiUrl)
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(formData))
                .retrieve()
                .bodyToMono(String.class)
                .map(this::parseScanResponse)
                .onErrorResume(error -> {
                    log.error("|MediaForProfile|callScanApi|failed|error={}", error.getMessage());
                    return Mono.just(new ScanResult(true));
                });
    }

    private ScanResult parseScanResponse(String rawResponse) {
        JsonObject json = GsonUtils.fromString(rawResponse);
        JsonObject data = json.getAsJsonObject("data");
        if (data == null) {
            return new ScanResult(true);
        }
        boolean isNsfw = data.has("is_nsfw") && data.get("is_nsfw").getAsBoolean();
        return new ScanResult(isNsfw);
    }

    private Mono<Void> sendAvatarScanEvent(String userId, String avatarUrl, String publicId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", userId);
        payload.addProperty("avatarUrl", avatarUrl);
        payload.addProperty("publicId", publicId);
        return sendKafka(CHECK_AVATAR_MEDIA_EVENT, userId, payload);
    }

    private Mono<Void> sendStoryScanEvent(UserStories story, String publicId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("storyId", story.getId());
        payload.addProperty("userId", story.getUserId());
        payload.addProperty("mediaUrl", story.getMediaUrl());
        payload.addProperty("musicUrl", story.getMusicUrl());
        payload.addProperty("musicStart", story.getMusicStart());
        payload.addProperty("musicEnd", story.getMusicEnd());
        payload.addProperty("publicId", publicId);
        return sendKafka(CHECK_STORY_MEDIA_EVENT, story.getId(), payload);
    }

    private Mono<Void> publishAvatarUpdateEvent(String userId, String avatarUrl, String mediaId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", userId);
        payload.addProperty("avatarUrl", avatarUrl);
        payload.addProperty("mediaId", mediaId);
        return sendKafka(AVATAR_UPDATE_EVENT, userId, payload);
    }

    private Mono<Void> publishStorySuccessEvent(UserStories story, String mediaId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("storyId", story.getId());
        payload.addProperty("userId", story.getUserId());
        payload.addProperty("mediaUrl", story.getMediaUrl());
        payload.addProperty("mediaType", story.getMediaType());
        payload.addProperty("musicUrl", story.getMusicUrl());
        payload.addProperty("musicStart", story.getMusicStart());
        payload.addProperty("musicEnd", story.getMusicEnd());
        payload.addProperty("musicTransformedUrl", resolveStoryMusicTransformedUrl(story));
        payload.addProperty("mediaId", mediaId);
        return sendKafka(STORY_SUCCESS_EVENT, story.getId(), payload);
    }

    private Mono<Void> sendKafka(String topic, String key, JsonObject payload) {
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(topic, key, payload.toString()),
                topic
        );
        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|MediaForProfile|sendKafka|topic={}|key={}|error={}", topic, key, error.getMessage()))
                .then();
    }

    private Mono<Void> sendAvatarSuccessSse(String userId, Media media) {
        JsonObject payload = baseSsePayload(userId, userId, OwnerType.AVATAR, media.getSecureUrl(), STATUS_APPROVED, "Avatar approved");
        payload.addProperty("mediaId", media.getAssetId());
        postSseService.sendToUser(userId, "avatar_upload_event", payload.toString());
        return Mono.empty();
    }

    private Mono<Void> sendStorySuccessSse(String userId, UserStories story, Media media) {
        JsonObject payload = baseSsePayload(userId, story.getId(), OwnerType.STORY, story.getMediaUrl(), STATUS_APPROVED, "Story approved");
        payload.addProperty("mediaId", media.getAssetId());
        payload.addProperty("musicUrl", story.getMusicUrl());
        payload.addProperty("musicStart", story.getMusicStart());
        payload.addProperty("musicEnd", story.getMusicEnd());
        payload.addProperty("musicTransformedUrl", resolveStoryMusicTransformedUrl(story));
        postSseService.sendToUser(userId, "story_upload_event", payload.toString());
        return Mono.empty();
    }

    private Mono<Void> sendProfileFailureSse(String userId, String eventName, String entityId, OwnerType ownerType, String mediaUrl, String message) {
        JsonObject payload = baseSsePayload(userId, entityId, ownerType, mediaUrl, STATUS_REJECTED, message);
        postSseService.sendToUser(userId, eventName, payload.toString());
        return Mono.empty();
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
        if (publicId == null || publicId.isBlank()) {
            return Mono.empty();
        }

        return Mono.fromCallable(() -> cloudinary.uploader().destroy(publicId, ObjectUtils.emptyMap()))
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> {
                    log.error("|MediaForProfile|deleteCloudinaryMedia|publicId={}|error={}", publicId, error.getMessage());
                    return Mono.empty();
                })
                .then();
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

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private void validateStoryMusicSegment(String musicUrl, Long musicStart, Long musicEnd) {
        if (musicStart == null && musicEnd == null) {
            return;
        }
        if (musicUrl == null || musicUrl.isBlank()) {
            throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, "musicUrl is required when musicStart or musicEnd is provided");
        }
        try {
            CloudinaryUtils.validateAudioSegment(musicStart, musicEnd);
        } catch (IllegalArgumentException ex) {
            throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, ex.getMessage(), ex);
        }
    }

    private String resolveStoryMusicTransformedUrl(UserStories story) {
        if (story == null || story.getMusicUrl() == null || story.getMusicUrl().isBlank()) {
            return null;
        }
        if (story.getMusicStart() == null && story.getMusicEnd() == null) {
            return story.getMusicUrl();
        }
        if (!CloudinaryUtils.isCloudinaryDeliveryUrl(story.getMusicUrl())) {
            return story.getMusicUrl();
        }
        try {
            return CloudinaryUtils.withAudioSegment(story.getMusicUrl(), story.getMusicStart(), story.getMusicEnd());
        } catch (IllegalArgumentException ex) {
            log.error("|MediaForProfile|resolveStoryMusicTransformedUrl|storyId={}|error={}", story.getId(), ex.getMessage());
            return story.getMusicUrl();
        }
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

    private String buildScanFilename(String publicId, String secureUrl) {
        String filename = basename(publicId);
        if (filename.isBlank()) {
            filename = basename(secureUrl);
        }
        if (filename.isBlank()) {
            filename = "media";
        }

        filename = filename.replaceAll("[^A-Za-z0-9._-]", "_");
        if (!filename.contains(".")) {
            filename = filename + resolveExtension(secureUrl);
        }
        return filename;
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

    private String resolveExtension(String secureUrl) {
        String filename = basename(secureUrl);
        int dotIndex = filename.lastIndexOf('.');
        if (dotIndex > 0 && dotIndex < filename.length() - 1) {
            String extension = filename.substring(dotIndex).toLowerCase(Locale.ROOT);
            if (extension.matches("\\.[a-z0-9]{1,8}")) {
                return extension;
            }
        }
        return ".jpg";
    }

    private String resolveMediaType(String mediaUrl) {
        String extension = resolveExtension(mediaUrl).toLowerCase(Locale.ROOT);
        return switch (extension) {
            case ".mp4", ".mov", ".webm", ".m4v" -> "VIDEO";
            default -> "IMAGE";
        };
    }

    private String resolveMediaTypeFromMedia(Media media, String fallbackUrl) {
        if (media != null && media.getResourceType() != null && !media.getResourceType().isBlank()) {
            return media.getResourceType().toUpperCase(Locale.ROOT);
        }
        return resolveMediaType(fallbackUrl);
    }

    private record ScanResult(boolean nsfw) {
    }
}
