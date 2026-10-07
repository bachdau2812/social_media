package com.dauducbach.clone.modules.post.stories.publishing;

import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.post.service.post.MediaModerationProvider;
import com.dauducbach.clone.modules.post.dto.story.request.StoryCreateRequest;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import com.dauducbach.clone.modules.post.stories.policy.StoryMusicSegmentPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryMediaServiceTest {
    @Mock
    UserIdentityQuery userIdentityQuery;
    @Mock
    UserStoriesRepository userStoriesRepository;
    @Mock
    MediaAssets mediaAssets;
    @Mock
    StoryPublicationMessaging publicationMessaging;
    @Mock
    StoryPublishingStore storyPublishingStore;
    @Mock
    MediaModerationProvider moderationProvider;
    @Mock
    AuditRecorder userAuditService;

    @Test
    void createStoryStoresMusicSegmentAndPublishesScanEvent() {
        StoryMediaService service = newService();
        when(userIdentityQuery.exists("user-1")).thenReturn(Mono.just(true));
        when(storyPublishingStore.createOrReuse(any(UserStories.class), eq(false)))
                .thenAnswer(invocation -> Mono.just(new StoryPublishingStore.Submission(invocation.getArgument(0), true)));
        when(publicationMessaging.requestMediaScan(any(UserStories.class), anyString())).thenReturn(Mono.empty());

        StepVerifier.create(service.createStory(new StoryCreateRequest(
                        "user-1",
                        "https://res.cloudinary.com/demo/image/upload/v1781617130/stories/story_1.jpg",
                        "https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3",
                        30L,
                        45L
                )))
                .assertNext(response -> {
                    assertThat(response.userId()).isEqualTo("user-1");
                    assertThat(response.ownerType()).isEqualTo(OwnerType.STORY.name());
                    assertThat(response.status()).isEqualTo("PENDING_SCAN");
                })
                .verifyComplete();

        ArgumentCaptor<UserStories> storyCaptor = ArgumentCaptor.forClass(UserStories.class);
        verify(publicationMessaging).requestMediaScan(storyCaptor.capture(), eq("stories/story_1"));
        assertThat(storyCaptor.getValue().getUserId()).isEqualTo("user-1");
        assertThat(storyCaptor.getValue().getMediaType()).isEqualTo("IMAGE");
        assertThat(storyCaptor.getValue().getMusicUrl())
                .isEqualTo("https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3");
        assertThat(storyCaptor.getValue().getMusicStart()).isEqualTo(30L);
        assertThat(storyCaptor.getValue().getMusicEnd()).isEqualTo(45L);
    }

    @Test
    void createStoryRejectsInvalidMusicSegment() {
        StoryMediaService service = newService();

        assertThatThrownBy(() -> service.createStory(new StoryCreateRequest(
                        "user-1",
                        "https://res.cloudinary.com/demo/image/upload/v1781617130/stories/story_1.jpg",
                        "https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3",
                        45L,
                        30L
                )))
                .hasMessage("Story music segment must be between 1 and 60 seconds");
    }

    @Test
    void createStoryReturnsExistingApprovedPublicationWithoutRepublishingScanEvent() {
        StoryMediaService service = newService();
        UserStories existing = story("story-1", "owner-1", "https://cdn.example/story.jpg", "APPROVED");
        existing.setPublicationId("publication-1");
        existing.setPublicationOrder(2);
        existing.setPublicationItemCount(3);

        when(userIdentityQuery.exists("owner-1")).thenReturn(Mono.just(true));
        when(storyPublishingStore.createOrReuse(any(UserStories.class), eq(true)))
                .thenReturn(Mono.just(new StoryPublishingStore.Submission(existing, false)));

        StepVerifier.create(service.createStory(new StoryCreateRequest(
                        "owner-1",
                        "https://cdn.example/story.jpg",
                        null,
                        null,
                        null,
                        null,
                        "publication-1",
                        2,
                        3
                )))
                .expectNextMatches(response -> "STORY".equals(response.ownerType())
                        && "story-1".equals(response.entityId())
                        && "APPROVED".equals(response.status())
                        && "Story is already approved".equals(response.message()))
                .verifyComplete();

        verify(publicationMessaging, never()).requestMediaScan(any(UserStories.class), anyString());
        verify(storyPublishingStore).createOrReuse(any(UserStories.class), eq(true));
    }

    @Test
    void handleStoryScanEventApprovesStorySendsSseAndPublishesSuccessPayload() {
        StoryMediaScanService service = newScanService();
        UserStories pendingStory = story("story-1", "owner-1", "https://cdn.example/story.jpg", "PENDING_SCAN");
        pendingStory.setMusicId("music-1");
        pendingStory.setMusicUrl("https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3");
        pendingStory.setMusicStart(5000L);
        pendingStory.setMusicEnd(15000L);
        pendingStory.setPublicationId("publication-1");
        pendingStory.setPublicationOrder(1);
        pendingStory.setPublicationItemCount(2);
        MediaAssetView savedMedia = new MediaAssetView("media-1", "stories/story_1", 0, 0,
                null, "image", 0, null, "https://cdn.example/story.jpg", "owner-1",
                OwnerType.STORY, null, null, null, null, null);

        stubStoryClaim(pendingStory);
        when(moderationProvider.scan("https://cdn.example/story.jpg", "stories/story_1", "IMAGE"))
                .thenReturn(Mono.just(MediaModerationProvider.Decision.APPROVED));
        when(mediaAssets.registerCloudinaryAsset("stories/story_1", "owner-1", OwnerType.STORY))
                .thenReturn(Mono.just(savedMedia));
        when(userStoriesRepository.save(any(UserStories.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(publicationMessaging.publishApproved(any(UserStories.class), eq(savedMedia), anyString()))
                .thenReturn(Mono.empty());
        when(mediaAssets.transformMusicUrlIfSupported(
                "https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3",
                5000L,
                15000L
        )).thenReturn("https://res.cloudinary.com/demo/video/upload/so_5,eo_15/v1234567890/musics/song.mp3");
        service.scan(storyScanRequest("https://cdn.example/story.jpg", "IMAGE")).block();

        ArgumentCaptor<UserStories> storyCaptor = ArgumentCaptor.forClass(UserStories.class);
        verify(userStoriesRepository).save(storyCaptor.capture());
        assertThat(storyCaptor.getValue().getStatus()).isEqualTo("APPROVED");
        assertThat(storyCaptor.getValue().getMediaType()).isEqualTo("IMAGE");

        verify(publicationMessaging).publishApproved(
                any(UserStories.class),
                eq(savedMedia),
                eq("https://res.cloudinary.com/demo/video/upload/so_5,eo_15/v1234567890/musics/song.mp3"));
    }

    @Test
    void handleStoryScanEventRejectsStoryDeletesCloudinaryMediaAndSendsFailureSse() {
        StoryMediaScanService service = newScanService();
        UserStories pendingStory = story("story-1", "owner-1", "https://cdn.example/story.jpg", "PENDING_SCAN");
        stubStoryClaim(pendingStory);

        when(moderationProvider.scan("https://cdn.example/story.jpg", "stories/story_1", "IMAGE"))
                .thenReturn(Mono.just(MediaModerationProvider.Decision.REJECTED));
        when(userStoriesRepository.save(any(UserStories.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(mediaAssets.deleteAsset("stories/story_1")).thenReturn(Mono.empty());
        when(publicationMessaging.publishRejected(
                eq("owner-1"), eq("story-1"), eq("https://cdn.example/story.jpg"),
                eq("Story rejected due to invalid media")))
                .thenReturn(Mono.empty());
        when(userAuditService.record(any())).thenReturn(Mono.empty());

        service.scan(storyScanRequest("https://cdn.example/story.jpg", "IMAGE")).block();

        ArgumentCaptor<UserStories> storyCaptor = ArgumentCaptor.forClass(UserStories.class);
        verify(userStoriesRepository).save(storyCaptor.capture());
        assertThat(storyCaptor.getValue().getStatus()).isEqualTo("REJECTED");
        verify(mediaAssets).deleteAsset("stories/story_1");
        verify(userAuditService).record(any());
        verify(publicationMessaging).publishRejected(
                "owner-1", "story-1", "https://cdn.example/story.jpg", "Story rejected due to invalid media");
    }

    @Test
    void handleStoryScanEventDelegatesVideoToBypassAwareModeration() {
        StoryMediaScanService service = newScanService();
        UserStories pendingStory = story(
                "story-1",
                "owner-1",
                "https://res.cloudinary.com/demo/video/upload/v1/story.mp4",
                "PENDING_SCAN");
        pendingStory.setMediaType("VIDEO");
        MediaAssetView savedMedia = new MediaAssetView("media-video", "stories/story_1", 0, 0,
                null, "video", 0, null, pendingStory.getMediaUrl(), "owner-1",
                OwnerType.STORY, null, null, null, null, null);

        stubStoryClaim(pendingStory);
        when(moderationProvider.scan(pendingStory.getMediaUrl(), "stories/story_1", "VIDEO"))
                .thenReturn(Mono.just(MediaModerationProvider.Decision.APPROVED));
        when(mediaAssets.registerCloudinaryAsset("stories/story_1", "owner-1", OwnerType.STORY))
                .thenReturn(Mono.just(savedMedia));
        when(userStoriesRepository.save(any(UserStories.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(publicationMessaging.publishApproved(any(UserStories.class), eq(savedMedia), nullable(String.class)))
                .thenReturn(Mono.empty());

        service.scan(storyScanRequest(pendingStory.getMediaUrl(), "VIDEO")).block();

        verify(moderationProvider).scan(pendingStory.getMediaUrl(), "stories/story_1", "VIDEO");
        verify(userStoriesRepository).save(any(UserStories.class));
    }

    @Test
    void releasesScanClaimWhenApprovalPersistenceFails() {
        StoryMediaScanService service = newScanService();
        UserStories pendingStory = story("story-1", "owner-1", "https://cdn.example/story.jpg", "PENDING_SCAN");
        stubStoryClaim(pendingStory);
        when(moderationProvider.scan(pendingStory.getMediaUrl(), "stories/story_1", "IMAGE"))
                .thenReturn(Mono.just(MediaModerationProvider.Decision.APPROVED));
        when(mediaAssets.registerCloudinaryAsset("stories/story_1", "owner-1", OwnerType.STORY))
                .thenReturn(Mono.just(new MediaAssetView("media-1", "stories/story_1", 0, 0,
                        null, "image", 0, null, pendingStory.getMediaUrl(), "owner-1",
                        OwnerType.STORY, null, null, null, null, null)));
        when(userStoriesRepository.save(any(UserStories.class)))
                .thenReturn(Mono.error(new IllegalStateException("story persistence failed")));
        when(userStoriesRepository.releaseScanClaim("story-1")).thenReturn(Mono.just(1));

        StepVerifier.create(service.scan(storyScanRequest(pendingStory.getMediaUrl(), "IMAGE")))
                .expectErrorMessage("story persistence failed")
                .verify();

        verify(userStoriesRepository).releaseScanClaim("story-1");
    }

    private StoryMediaService newService() {
        return new StoryMediaService(
                userIdentityQuery,
                storyPublishingStore,
                publicationMessaging,
                new StoryMusicSegmentPolicy()
        );
    }

    private StoryMediaScanService newScanService() {
        return new StoryMediaScanService(
                userStoriesRepository,
                mediaAssets,
                publicationMessaging,
                moderationProvider,
                userAuditService
        );
    }

    private void stubStoryClaim(UserStories pendingStory) {
        when(userStoriesRepository.claimPendingScan(pendingStory.getId())).thenReturn(Mono.just(1));
        when(userStoriesRepository.findById(pendingStory.getId())).thenReturn(Mono.just(pendingStory));
    }

    private StoryMediaScanRequest storyScanRequest(String mediaUrl, String mediaType) {
        return new StoryMediaScanRequest("story-1", "owner-1", mediaUrl, "stories/story_1", mediaType);
    }

    private UserStories story(String storyId, String userId, String mediaUrl, String status) {
        return UserStories.builder()
                .id(storyId)
                .userId(userId)
                .mediaUrl(mediaUrl)
                .mediaType("IMAGE")
                .status(status)
                .createdAt(Instant.now())
                .expiredAt(Instant.now().plusSeconds(3600))
                .build();
    }
}
