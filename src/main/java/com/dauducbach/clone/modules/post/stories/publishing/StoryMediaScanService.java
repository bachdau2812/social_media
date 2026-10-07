package com.dauducbach.clone.modules.post.stories.publishing;

import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import com.dauducbach.clone.modules.post.service.post.MediaModerationProvider;
import com.dauducbach.clone.modules.post.stories.publishing.StoryPublicationMessaging;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Locale;

@Service
public class StoryMediaScanService {
    private static final Logger log = LoggerFactory.getLogger(StoryMediaScanService.class);
    private static final String STATUS_APPROVED = "APPROVED";
    private static final String STATUS_REJECTED = "REJECTED";

    private final UserStoriesRepository userStoriesRepository;
    private final MediaAssets mediaAssets;
    private final StoryPublicationMessaging publicationMessaging;
    private final MediaModerationProvider moderationProvider;
    private final AuditRecorder auditRecorder;

    public StoryMediaScanService(
            UserStoriesRepository userStoriesRepository,
            MediaAssets mediaAssets,
            StoryPublicationMessaging publicationMessaging,
            MediaModerationProvider moderationProvider,
            AuditRecorder auditRecorder
    ) {
        this.userStoriesRepository = userStoriesRepository;
        this.mediaAssets = mediaAssets;
        this.publicationMessaging = publicationMessaging;
        this.moderationProvider = moderationProvider;
        this.auditRecorder = auditRecorder;
    }

    public Mono<Void> scan(StoryMediaScanRequest request) {
        String storyId = request.storyId();
        String userId = request.userId();
        String mediaUrl = request.mediaUrl();
        String publicId = request.publicId();
        String mediaType = request.mediaType();
        return moderationProvider.scan(mediaUrl, publicId, mediaType)
                .doOnSuccess(decision -> log.info("|MediaForProfile|handleStoryScanEvent|moderation result|storyId={}|userId={}|decision={}",
                        storyId, userId, decision))
                .flatMap(decision -> decision == MediaModerationProvider.Decision.REJECTED
                        ? handleStoryFailed(storyId, userId, mediaUrl, publicId)
                        : handleStorySuccess(storyId, userId, mediaUrl, publicId))
                .doOnSuccess(v -> log.info("|MediaForProfile|handleStoryScanEvent|completed|storyId={}", storyId))
                .doOnError(error -> log.error("|StoryMediaScanService|scan|failed|storyId={}|error={}", storyId, error.getMessage()));
    }

    private Mono<Void> handleStorySuccess(String storyId, String userId, String mediaUrl, String publicId) {
        log.info("|MediaForProfile|handleStorySuccess|storyId={}|userId={}|publicId={}", storyId, userId, publicId);
        return claimPendingStory(storyId)
                .flatMap(story -> mediaAssets.registerCloudinaryAsset(publicId, userId, OwnerType.STORY)
                        .flatMap(media -> {
                            story.setStatus(STATUS_APPROVED);
                            story.setMediaType(resolveMediaTypeFromMedia(media, mediaUrl));
                            return userStoriesRepository.save(story)
                                    .flatMap(saved -> publicationMessaging.publishApproved(
                                            saved, media, resolveStoryMusicTransformedUrl(saved)));
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
                            .then(mediaAssets.deleteAsset(publicId))
                            .then(publicationMessaging.publishRejected(
                                    userId, storyId, mediaUrl, "Story rejected due to invalid media"))
                            .then(saveProfileMediaAudit(userId, AuditActionType.UPLOAD_STORY,
                                    "STORY", storyId, "FAILURE", publicId));
                })
                .onErrorResume(error -> releaseStoryScanClaim(storyId).then(Mono.error(error)));
    }

    private Mono<UserStories> claimPendingStory(String storyId) {
        return userStoriesRepository.claimPendingScan(storyId)
                .filter(updated -> updated > 0)
                .flatMap(updated -> userStoriesRepository.findById(storyId));
    }

    private Mono<Void> releaseStoryScanClaim(String storyId) {
        return userStoriesRepository.releaseScanClaim(storyId).then();
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
        return auditRecorder.record(new AuditEntry(userId, action, resourceType, resourceId,
                status, metadata.toString(), null));
    }

    private String resolveStoryMusicTransformedUrl(UserStories story) {
        if (story.getMusicUrl() == null || story.getMusicUrl().isBlank()) return null;
        if (story.getMusicStart() == null && story.getMusicEnd() == null) return story.getMusicUrl();
        try {
            return mediaAssets.transformMusicUrlIfSupported(
                    story.getMusicUrl(), story.getMusicStart(), story.getMusicEnd());
        } catch (IllegalArgumentException ex) {
            log.error("|MediaForProfile|resolveStoryMusicTransformedUrl|storyId={}|error={}", story.getId(), ex.getMessage());
            return story.getMusicUrl();
        }
    }

    private String resolveMediaTypeFromMedia(MediaAssetView media, String fallbackUrl) {
        if (media != null && media.resourceType() != null && !media.resourceType().isBlank()) {
            return media.resourceType().toUpperCase(Locale.ROOT);
        }
        return StoryMediaMetadata.resolveMediaType(fallbackUrl);
    }
}
