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
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.service.PostSseService;
import com.dauducbach.clone.modules.user.dto.request.AvatarUploadRequest;
import com.dauducbach.clone.modules.user.dto.request.MusicSelectRequest;
import com.dauducbach.clone.modules.user.dto.request.StoryCreateRequest;
import com.dauducbach.clone.modules.user.dto.response.ProfileMediaUploadResponse;
import com.dauducbach.clone.modules.user.entity.Musics;
import com.dauducbach.clone.modules.user.entity.UserMusics;
import com.dauducbach.clone.modules.user.entity.StoryView;
import com.dauducbach.clone.modules.user.entity.UserStories;
import com.dauducbach.clone.modules.user.repositoty.MusicsRepository;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.user.repositoty.UserStoriesRepository;
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
import org.springframework.data.domain.PageRequest;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
    private static final String STATUS_PROCESSING = "PROCESSING_SCAN";
    private static final String STATUS_APPROVED = "APPROVED";
    private static final String STATUS_REJECTED = "REJECTED";
    private static final Duration STORY_TTL = Duration.ofHours(24);

    UserDetailsRepository userDetailsRepository;
    MusicsRepository musicsRepository;
    UserStoriesRepository userStoriesRepository;
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

    public Mono<ProfileMediaUploadResponse> createStory(StoryCreateRequest request) {
        String userId = normalizeRequired(request.userId(), "userId");
        String mediaUrl = normalizeRequired(request.mediaUrl(), "mediaUrl");
        String musicId = normalizeOptional(request.musicId());
        String musicUrl = normalizeOptional(request.musicUrl());
        Long musicStart = request.musicStart();
        Long musicEnd = request.musicEnd();
        validateStoryMusicSegment(firstNonBlank(musicId, musicUrl), musicStart, musicEnd);
        String publicId = resolvePublicId(mediaUrl);
        String storyId = UUID.randomUUID().toString();
        boolean explicitPublication = request.publicationId() != null && !request.publicationId().isBlank();
        String publicationId = firstNonBlank(request.publicationId(), storyId);
        int publicationItemCount = request.publicationItemCount() == null ? 1 : request.publicationItemCount();
        int publicationOrder = request.publicationOrder() == null ? 1 : request.publicationOrder();
        validateStoryPublication(publicationOrder, publicationItemCount);
        Instant now = Instant.now();

        log.info("|MediaForProfile|createStory|userId={}|storyId={}|publicId={}|mediaType={}|hasMusic={}",
                userId, storyId, publicId, resolveMediaType(mediaUrl), firstNonBlank(musicId, musicUrl) != null);
        UserStories story = UserStories.builder()
                .id(storyId)
                .userId(userId)
                .mediaUrl(mediaUrl)
                .mediaType(resolveMediaType(mediaUrl))
                .musicId(musicId)
                .musicUrl(musicUrl)
                .musicStart(musicStart)
                .musicEnd(musicEnd)
                .publicationId(publicationId)
                .publicationOrder(publicationOrder)
                .publicationItemCount(publicationItemCount)
                .status(STATUS_PENDING)
                .createdAt(now)
                .expiredAt(now.plus(STORY_TTL))
                .build();

        Mono<StorySubmission> existing = explicitPublication
                ? userStoriesRepository.findByUserIdAndPublicationIdAndPublicationOrder(userId, publicationId, publicationOrder)
                    .map(found -> new StorySubmission(found, false))
                : Mono.empty();
        Mono<StorySubmission> insertOnce = Mono.defer(() -> r2dbcEntityTemplate.insert(UserStories.class).using(story)
                .doOnSuccess(saved -> log.info("|MediaForProfile|createStory|saved pending|storyId={}|userId={}",
                        saved.getId(), saved.getUserId()))
                .map(saved -> new StorySubmission(saved, true))
                .onErrorResume(DataIntegrityViolationException.class, error ->
                        userStoriesRepository.findByUserIdAndPublicationIdAndPublicationOrder(userId, publicationId, publicationOrder)
                                .map(found -> new StorySubmission(found, false))
                                .switchIfEmpty(Mono.error(error))));

        return ensureUserExists(userId)
                .then(existing.switchIfEmpty(insertOnce))
                .flatMap(submission -> {
                    UserStories saved = submission.story();
                    if (!STATUS_REJECTED.equalsIgnoreCase(saved.getStatus())) return Mono.just(submission);
                    saved.setMediaUrl(mediaUrl);
                    saved.setMediaType(resolveMediaType(mediaUrl));
                    saved.setMusicId(musicId);
                    saved.setMusicUrl(musicUrl);
                    saved.setMusicStart(musicStart);
                    saved.setMusicEnd(musicEnd);
                    saved.setPublicationItemCount(publicationItemCount);
                    saved.setStatus(STATUS_PENDING);
                    saved.setCreatedAt(now);
                    saved.setExpiredAt(now.plus(STORY_TTL));
                    return userStoriesRepository.save(saved).map(retried -> new StorySubmission(retried, true));
                })
                .flatMap(submission -> submission.shouldScan() && STATUS_PENDING.equalsIgnoreCase(submission.story().getStatus())
                        ? sendStoryScanEvent(submission.story(), resolvePublicId(submission.story().getMediaUrl())).thenReturn(submission.story())
                        : Mono.just(submission.story()))
                .map(saved -> new ProfileMediaUploadResponse(
                        userId,
                        OwnerType.STORY.name(),
                        saved.getId(),
                        saved.getStatus(),
                        STATUS_APPROVED.equalsIgnoreCase(saved.getStatus())
                                ? "Story is already approved"
                                : "Story is waiting for media validation"))
                .doOnSuccess(response -> log.info("|MediaForProfile|createStory|status={}|storyId={}|userId={}", response.status(), response.entityId(), userId))
                .doOnError(error -> log.error("|MediaForProfile|createStory|failed|storyId={}|userId={}|error={}",
                        storyId, userId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.STORY_SAVE_FAILED, "Create story failed", error));
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

    public Mono<PageResponse<UserStories>> getStories(String userId, int page, int size) {
        return getStories(userId, userId, page, size);
    }

    public Mono<PageResponse<UserStories>> getStories(String userId, String viewerId, int page, int size) {
        if (userId == null || userId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "userId is required"));
        }
        if (viewerId == null || viewerId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.AUTHENTICATION_FAILED, "Authenticated viewer is required"));
        }

        int pageNumber = Math.max(page, 0);
        int pageSize = Math.clamp(size, 1, 100);
        boolean owner = userId.equals(viewerId);
        long offset = (long) pageNumber * pageSize;
        Instant now = Instant.now();
        Mono<Long> total = owner
                ? userStoriesRepository.countByUserIdAndStatus(userId, STATUS_APPROVED)
                : userStoriesRepository.countActiveApprovedByUserId(userId, now);
        Flux<UserStories> content = owner
                ? userStoriesRepository.findByUserIdAndStatusOrderByCreatedAtDesc(userId, STATUS_APPROVED, PageRequest.of(pageNumber, pageSize))
                : userStoriesRepository.findActiveApprovedByUserId(userId, now, pageSize, offset);
        log.info("|MediaForProfile|getStories|userId={}|viewerId={}|owner={}|page={}|size={}", userId, viewerId, owner, pageNumber, pageSize);
        return total.flatMap(count -> content.collectList()
                        .flatMap(stories -> hydrateViewerSeen(stories, viewerId, owner))
                        .doOnSuccess(stories -> log.info("|MediaForProfile|getStories|dbResult|userId={}|count={}|total={}",
                                userId, stories.size(), count))
                        .map(stories -> PageResponse.of(stories, pageNumber, count, pageSize)))
                .doOnError(error -> log.error("|MediaForProfile|getStories|failed|userId={}|error={}",
                        userId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException ? error : new AppException(
                        ErrorCode.STORY_SAVE_FAILED,
                        String.format("Fetch stories failed for userId=%s", userId),
                        error
                ));
    }

    public Mono<PageResponse<UserStories>> getStories(String userId, int page, int size, MediaDisplayType mediaType) {
        return getStories(userId, userId, page, size, mediaType);
    }

    public Mono<PageResponse<UserStories>> getStories(String userId, String viewerId, int page, int size, MediaDisplayType mediaType) {
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.STORY : mediaType;
        return getStories(userId, viewerId, page, size)
                .map(response -> new PageResponse<>(
                        response.content().stream()
                                .map(story -> transformStoryForDisplay(story, displayType))
                                .toList(),
                        response.pageNumber(),
                        response.totalElements(),
                        response.totalPages()
                ));
    }

    private Mono<List<UserStories>> hydrateViewerSeen(List<UserStories> stories, String viewerId, boolean owner) {
        if (stories.isEmpty()) return Mono.just(stories);
        if (owner) {
            stories.forEach(story -> story.setViewerSeen(true));
            return Mono.just(stories);
        }
        List<String> storyIds = stories.stream().map(UserStories::getId).toList();
        return r2dbcEntityTemplate.select(StoryView.class)
                .matching(Query.query(Criteria.where("viewerId").is(viewerId).and("storyId").in(storyIds)))
                .all()
                .map(StoryView::getStoryId)
                .collectList()
                .map(viewedIds -> {
                    stories.forEach(story -> story.setViewerSeen(viewedIds.contains(story.getId())));
                    return stories;
                });
    }

    private Media transformMediaForDisplay(Media media, MediaDisplayType mediaType) {
        media.setUrl(cloudinaryMediaService.transformDeliveryUrl(media.getUrl(), mediaType));
        media.setSecureUrl(cloudinaryMediaService.transformDeliveryUrl(media.getSecureUrl(), mediaType));
        return media;
    }

    private UserStories transformStoryForDisplay(UserStories story, MediaDisplayType mediaType) {
        story.setMediaUrl(cloudinaryMediaService.transformDeliveryUrl(story.getMediaUrl(), mediaType));
        return story;
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

    @KafkaListener(topics = CHECK_STORY_MEDIA_EVENT, groupId = "user-service")
    public CompletableFuture<Void> handleStoryScanEvent(@Payload String payload) {
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String storyId = KafkaUtils.extractString(payloadJson, "storyId");
        String userId = KafkaUtils.extractString(payloadJson, "userId");
        String mediaUrl = KafkaUtils.extractString(payloadJson, "mediaUrl");
        String publicId = KafkaUtils.extractString(payloadJson, "publicId");
        log.info("|MediaForProfile|handleStoryScanEvent|received|storyId={}|userId={}|publicId={}",
                storyId, userId, publicId);

        if (storyId.isBlank() || userId.isBlank() || mediaUrl.isBlank()) {
            log.warn("|MediaForProfile|handleStoryScanEvent|missing data|storyId={}|userId={}", storyId, userId);
            return CompletableFuture.completedFuture(null);
        }

        return mediaScanUtils.scanMedia(mediaUrl, publicId)
                .doOnSuccess(result -> log.info("|MediaForProfile|handleStoryScanEvent|scan result|storyId={}|userId={}|nsfw={}",
                        storyId, userId, result.nsfw()))
                .flatMap(result -> result.nsfw()
                        ? handleStoryFailed(storyId, userId, mediaUrl, publicId)
                        : handleStorySuccess(storyId, userId, mediaUrl, publicId))
                .doOnSuccess(v -> log.info("|MediaForProfile|handleStoryScanEvent|completed|storyId={}", storyId))
                .doOnError(error -> log.error("|MediaForProfile|handleStoryScanEvent|failed|storyId={}|error={}", storyId, error.getMessage()))
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

    private Mono<Void> handleStorySuccess(String storyId, String userId, String mediaUrl, String publicId) {
        log.info("|MediaForProfile|handleStorySuccess|storyId={}|userId={}|publicId={}", storyId, userId, publicId);
        return claimPendingStory(storyId)
                .flatMap(story -> mediaService.saveCloudinaryMedia(publicId, userId, OwnerType.STORY)
                        .flatMap(media -> {
                            story.setStatus(STATUS_APPROVED);
                            story.setMediaType(resolveMediaTypeFromMedia(media, mediaUrl));
                            return userStoriesRepository.save(story)
                                    .flatMap(saved -> sendStorySuccessSse(userId, saved, media)
                                            .then(publishStorySuccessEvent(saved, media.getAssetId())));
                        }))
                .onErrorResume(error -> releaseStoryScanClaim(storyId).then(Mono.error(error)))
                .doOnSuccess(v -> log.info("|MediaForProfile|handleStorySuccess|completed|storyId={}|userId={}", storyId, userId));
    }

    private Mono<Void> handleStoryFailed(String storyId, String userId, String mediaUrl, String publicId) {
        log.warn("|MediaForProfile|handleStoryFailed|storyId={}|userId={}|publicId={}", storyId, userId, publicId);
        return claimPendingStory(storyId)
                .flatMap(story -> {
                    story.setStatus(STATUS_REJECTED);
                    return userStoriesRepository.save(story)
                            .then(deleteCloudinaryMedia(publicId))
                            .then(sendProfileFailureSse(userId, "story_upload_event", storyId, OwnerType.STORY, mediaUrl, "Story rejected due to invalid media"))
                            .then(saveProfileMediaAudit(userId, AuditActionType.UPLOAD_STORY, "STORY", storyId, "FAILURE", publicId));
                })
                .onErrorResume(error -> releaseStoryScanClaim(storyId).then(Mono.error(error)));
    }

    private Mono<UserStories> claimPendingStory(String storyId) {
        return r2dbcEntityTemplate.getDatabaseClient().sql("""
                        UPDATE user_stories
                        SET status = :processing
                        WHERE id = :storyId AND status = :pending
                        """)
                .bind("processing", STATUS_PROCESSING)
                .bind("storyId", storyId)
                .bind("pending", STATUS_PENDING)
                .fetch()
                .rowsUpdated()
                .filter(updated -> updated > 0)
                .flatMap(updated -> userStoriesRepository.findById(storyId));
    }

    private Mono<Void> releaseStoryScanClaim(String storyId) {
        return r2dbcEntityTemplate.getDatabaseClient().sql("""
                        UPDATE user_stories
                        SET status = :pending
                        WHERE id = :storyId AND status = :processing
                        """)
                .bind("pending", STATUS_PENDING)
                .bind("storyId", storyId)
                .bind("processing", STATUS_PROCESSING)
                .fetch()
                .rowsUpdated()
                .then();
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

    private Mono<Void> sendStoryScanEvent(UserStories story, String publicId) {
        JsonObject payload = new JsonObject();
        payload.addProperty("storyId", story.getId());
        payload.addProperty("userId", story.getUserId());
        payload.addProperty("mediaUrl", story.getMediaUrl());
                payload.addProperty("musicId", story.getMusicId());
        payload.addProperty("musicUrl", story.getMusicUrl());
        payload.addProperty("musicStart", story.getMusicStart());
        payload.addProperty("musicEnd", story.getMusicEnd());
        payload.addProperty("publicationId", story.getPublicationId());
        payload.addProperty("publicationOrder", story.getPublicationOrder());
        payload.addProperty("publicationItemCount", story.getPublicationItemCount());
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
                payload.addProperty("musicId", story.getMusicId());
        payload.addProperty("musicUrl", story.getMusicUrl());
        payload.addProperty("musicStart", story.getMusicStart());
        payload.addProperty("musicEnd", story.getMusicEnd());
        payload.addProperty("publicationId", story.getPublicationId());
        payload.addProperty("publicationOrder", story.getPublicationOrder());
        payload.addProperty("publicationItemCount", story.getPublicationItemCount());
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
                .doOnComplete(() -> log.info("|MediaForProfile|sendKafka|sent|topic={}|key={}", topic, key))
                .then();
    }

    private Mono<Void> sendAvatarSuccessSse(String userId, Media media) {
        JsonObject payload = baseSsePayload(userId, userId, OwnerType.AVATAR, media.getSecureUrl(), STATUS_APPROVED, "Avatar approved");
        payload.addProperty("mediaId", media.getAssetId());
        return postSseService.sendToUser(userId, "avatar_upload_event", payload.toString())
                .doOnSuccess(unused -> log.info("|MediaForProfile|sendAvatarSuccessSse|sent|userId={}|mediaId={}", userId, media.getAssetId()));
    }

    private Mono<Void> sendStorySuccessSse(String userId, UserStories story, Media media) {
        JsonObject payload = baseSsePayload(userId, story.getId(), OwnerType.STORY, story.getMediaUrl(), STATUS_APPROVED, "Story approved");
        payload.addProperty("mediaId", media.getAssetId());
        payload.addProperty("musicId", story.getMusicId());
        payload.addProperty("musicUrl", story.getMusicUrl());
        payload.addProperty("musicStart", story.getMusicStart());
        payload.addProperty("musicEnd", story.getMusicEnd());
        payload.addProperty("musicTransformedUrl", resolveStoryMusicTransformedUrl(story));
        return postSseService.sendToUser(userId, "story_upload_event", payload.toString())
                .doOnSuccess(unused -> log.info("|MediaForProfile|sendStorySuccessSse|sent|userId={}|storyId={}|mediaId={}",
                        userId, story.getId(), media.getAssetId()));
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

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private void validateStoryPublication(int order, int itemCount) {
        if (itemCount < 1 || order < 1 || order > itemCount) {
            throw new AppException(ErrorCode.STORY_SAVE_FAILED, "Invalid Story publication order");
        }
    }

    private void validateStoryMusicSegment(String musicReference, Long musicStart, Long musicEnd) {
        if (musicStart == null && musicEnd == null) {
            return;
        }
        if (musicReference == null || musicReference.isBlank()) {
            throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, "musicId is required when musicStart or musicEnd is provided");
        }
        try {
            cloudinaryMediaService.validateMusicSegment(musicStart, musicEnd);
        } catch (IllegalArgumentException ex) {
            throw new AppException(ErrorCode.PROFILE_MEDIA_INVALID, ex.getMessage(), ex);
        }
    }
    private String firstNonBlank(String first, String second) {
        String normalizedFirst = normalizeOptional(first);
        return normalizedFirst != null ? normalizedFirst : normalizeOptional(second);
    }

    private String resolveStoryMusicTransformedUrl(UserStories story) {
        if (story == null || story.getMusicUrl() == null || story.getMusicUrl().isBlank()) {
            return null;
        }
        if (story.getMusicStart() == null && story.getMusicEnd() == null) {
            return story.getMusicUrl();
        }
        try {
            return cloudinaryMediaService.transformMusicUrlIfSupported(
                    story.getMusicUrl(), story.getMusicStart(), story.getMusicEnd());
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

    private record StorySubmission(UserStories story, boolean shouldScan) {
    }
}
