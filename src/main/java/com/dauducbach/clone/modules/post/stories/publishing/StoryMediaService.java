package com.dauducbach.clone.modules.post.stories.publishing;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.post.dto.story.request.StoryCreateRequest;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.stories.policy.StoryExpiryPolicy;
import com.dauducbach.clone.modules.post.stories.policy.StoryMusicSegmentPolicy;
import com.dauducbach.clone.modules.post.stories.publishing.StoryPublishingStore.Submission;
import com.dauducbach.clone.modules.user.dto.response.ProfileMediaUploadResponse;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
public class StoryMediaService {
    private static final Logger log = LoggerFactory.getLogger(StoryMediaService.class);
    private static final String STATUS_PENDING = "PENDING_SCAN";
    private static final String STATUS_APPROVED = "APPROVED";
    private static final String STATUS_REJECTED = "REJECTED";

    private final UserIdentityQuery userIdentityQuery;
    private final StoryPublishingStore storyPublishingStore;
    private final StoryPublicationMessaging publicationMessaging;
    private final StoryMusicSegmentPolicy storyMusicSegmentPolicy;

    public StoryMediaService(
            UserIdentityQuery userIdentityQuery,
            StoryPublishingStore storyPublishingStore,
            StoryPublicationMessaging publicationMessaging,
            StoryMusicSegmentPolicy storyMusicSegmentPolicy
    ) {
        this.userIdentityQuery = userIdentityQuery;
        this.storyPublishingStore = storyPublishingStore;
        this.publicationMessaging = publicationMessaging;
        this.storyMusicSegmentPolicy = storyMusicSegmentPolicy;
    }

    public Mono<ProfileMediaUploadResponse> createStory(StoryCreateRequest request) {
        String userId = normalizeRequired(request.userId(), "userId");
        String mediaUrl = normalizeRequired(request.mediaUrl(), "mediaUrl");
        String musicId = normalizeOptional(request.musicId());
        String musicUrl = normalizeOptional(request.musicUrl());
        Long musicStart = request.musicStart();
        Long musicEnd = request.musicEnd();
        storyMusicSegmentPolicy.validate(firstNonBlank(musicId, musicUrl), musicStart, musicEnd);

        String storyId = UUID.randomUUID().toString();
        boolean explicitPublication = request.publicationId() != null && !request.publicationId().isBlank();
        String publicationId = firstNonBlank(request.publicationId(), storyId);
        int publicationItemCount = request.publicationItemCount() == null ? 1 : request.publicationItemCount();
        int publicationOrder = request.publicationOrder() == null ? 1 : request.publicationOrder();
        validateStoryPublication(publicationOrder, publicationItemCount);
        Instant now = Instant.now();
        String mediaType = StoryMediaMetadata.resolveMediaType(mediaUrl);
        String publicId = StoryMediaMetadata.resolvePublicId(mediaUrl);

        log.info("|MediaForProfile|createStory|userId={}|storyId={}|publicId={}|mediaType={}|hasMusic={}",
                userId, storyId, publicId, mediaType, firstNonBlank(musicId, musicUrl) != null);
        UserStories story = UserStories.builder()
                .id(storyId)
                .userId(userId)
                .mediaUrl(mediaUrl)
                .mediaType(mediaType)
                .musicId(musicId)
                .musicUrl(musicUrl)
                .musicStart(musicStart)
                .musicEnd(musicEnd)
                .publicationId(publicationId)
                .publicationOrder(publicationOrder)
                .publicationItemCount(publicationItemCount)
                .status(STATUS_PENDING)
                .createdAt(now)
                .expiredAt(StoryExpiryPolicy.expirationFrom(now))
                .build();

        return ensureUserExists(userId)
                .then(storyPublishingStore.createOrReuse(story, explicitPublication)
                        .doOnNext(submission -> {
                            if (submission.shouldScan()) {
                                log.info("|MediaForProfile|createStory|saved pending|storyId={}|userId={}",
                                        submission.story().getId(), submission.story().getUserId());
                            }
                        }))
                .flatMap(submission -> refreshRejectedStory(
                        submission, mediaUrl, mediaType, musicId, musicUrl, musicStart, musicEnd,
                        publicationItemCount, now))
                .flatMap(submission -> submission.shouldScan()
                        && STATUS_PENDING.equalsIgnoreCase(submission.story().getStatus())
                        ? sendStoryScanEvent(submission.story(), StoryMediaMetadata.resolvePublicId(submission.story().getMediaUrl()))
                                .thenReturn(submission.story())
                        : Mono.just(submission.story()))
                .map(saved -> new ProfileMediaUploadResponse(
                        userId,
                        OwnerType.STORY.name(),
                        saved.getId(),
                        saved.getStatus(),
                        STATUS_APPROVED.equalsIgnoreCase(saved.getStatus())
                                ? "Story is already approved"
                                : "Story is waiting for media validation"))
                .doOnSuccess(response -> log.info("|MediaForProfile|createStory|status={}|storyId={}|userId={}",
                        response.status(), response.entityId(), userId))
                .doOnError(error -> log.error("|MediaForProfile|createStory|failed|storyId={}|userId={}|error={}",
                        storyId, userId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.STORY_SAVE_FAILED, "Create story failed", error));
    }

    private Mono<Submission> refreshRejectedStory(
            Submission submission,
            String mediaUrl,
            String mediaType,
            String musicId,
            String musicUrl,
            Long musicStart,
            Long musicEnd,
            int publicationItemCount,
            Instant now
    ) {
        UserStories story = submission.story();
        if (!STATUS_REJECTED.equalsIgnoreCase(story.getStatus())) return Mono.just(submission);
        story.setMediaUrl(mediaUrl);
        story.setMediaType(mediaType);
        story.setMusicId(musicId);
        story.setMusicUrl(musicUrl);
        story.setMusicStart(musicStart);
        story.setMusicEnd(musicEnd);
        story.setPublicationItemCount(publicationItemCount);
        story.setStatus(STATUS_PENDING);
        story.setCreatedAt(now);
        story.setExpiredAt(StoryExpiryPolicy.expirationFrom(now));
        return storyPublishingStore.save(story).map(retried -> new Submission(retried, true));
    }

    private Mono<Void> sendStoryScanEvent(UserStories story, String publicId) {
        return publicationMessaging.requestMediaScan(story, publicId)
                .doOnError(error -> log.error("|MediaForProfile|sendStoryScanEvent|storyId={}|error={}",
                        story.getId(), error.getMessage()));
    }

    private Mono<Void> ensureUserExists(String userId) {
        return userIdentityQuery.exists(userId)
                .flatMap(exists -> Boolean.TRUE.equals(exists)
                        ? Mono.empty()
                        : Mono.error(new AppException(ErrorCode.USER_DETAILS_NOT_FOUND,
                                String.format("User details not found for userId=%s", userId))));
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

    private String firstNonBlank(String first, String second) {
        String normalizedFirst = normalizeOptional(first);
        return normalizedFirst != null ? normalizedFirst : normalizeOptional(second);
    }

}
