package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.service.PostSseService;
import com.dauducbach.clone.modules.user.dto.request.StoryCreateRequest;
import com.dauducbach.clone.modules.user.entity.StoryView;
import com.dauducbach.clone.modules.user.entity.UserStories;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.user.repositoty.UserStoriesRepository;
import com.dauducbach.clone.modules.user.service.StoryMusicSegmentPolicy;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.MediaScanUtils;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import org.springframework.data.r2dbc.core.ReactiveSelectOperation;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.FetchSpec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryMediaServiceTest {
    @Mock
    UserDetailsRepository userDetailsRepository;
    @Mock
    UserStoriesRepository userStoriesRepository;
    @Mock
    MediaService mediaService;
    @Mock
    MediaCompatibilityFacade cloudinaryMediaService;
    @Mock
    PostSseService postSseService;
    @Mock
    KafkaSender<String, String> kafkaSender;
    @Mock
    R2dbcEntityTemplate r2dbcEntityTemplate;
    @Mock
    MediaScanUtils mediaScanUtils;
    @Mock
    UserAuditService userAuditService;

    @Test
    void createStoryStoresMusicSegmentAndPublishesScanEvent() {
        StoryMediaService service = newService();
        stubStoryInsert();
        when(userDetailsRepository.existsById("user-1")).thenReturn(Mono.just(true));
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> publisher = invocation.getArgument(0);
            StepVerifier.create(Flux.from(publisher))
                    .assertNext(record -> {
                        assertThat(record.topic()).isEqualTo("check_story_media_event");
                        JsonObject payload = GsonUtils.fromString(record.value());
                        assertThat(payload.get("userId").getAsString()).isEqualTo("user-1");
                        assertThat(payload.get("publicId").getAsString()).isEqualTo("stories/story_1");
                        assertThat(payload.get("musicUrl").getAsString()).isEqualTo("https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3");
                        assertThat(payload.get("musicStart").getAsLong()).isEqualTo(30L);
                        assertThat(payload.get("musicEnd").getAsLong()).isEqualTo(45L);
                    })
                    .verifyComplete();
            return Flux.empty();
        });

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

        when(userDetailsRepository.existsById("owner-1")).thenReturn(Mono.just(true));
        when(userStoriesRepository.findByUserIdAndPublicationIdAndPublicationOrder("owner-1", "publication-1", 2))
                .thenReturn(Mono.just(existing));

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

        verify(kafkaSender, never()).send(any(Publisher.class));
        verify(r2dbcEntityTemplate, never()).insert(UserStories.class);
    }

    @Test
    void getStoriesHydratesViewerSeenAndTransformsMediaUrl() {
        StoryMediaService service = newService();
        UserStories story = story("story-1", "owner-1", "https://cdn.example/story.jpg", "APPROVED");
        stubStoryViewSelect(StoryView.builder().storyId("story-1").viewerId("viewer-1").build());

        when(userStoriesRepository.countActiveApprovedByUserId(eq("owner-1"), any(Instant.class)))
                .thenReturn(Mono.just(1L));
        when(userStoriesRepository.findActiveApprovedByUserId(eq("owner-1"), any(Instant.class), eq(20), eq(0L)))
                .thenReturn(Flux.just(story));
        when(cloudinaryMediaService.transformDeliveryUrl("https://cdn.example/story.jpg", MediaDisplayType.STORY))
                .thenReturn("https://cdn.example/story-transformed.jpg");

        StepVerifier.create(service.getStories("owner-1", "viewer-1", 0, 20, MediaDisplayType.STORY))
                .assertNext(response -> {
                    assertThat(response.content()).hasSize(1);
                    assertThat(response.content().getFirst().getId()).isEqualTo("story-1");
                    assertThat(response.content().getFirst().getViewerSeen()).isTrue();
                    assertThat(response.content().getFirst().getMediaUrl()).isEqualTo("https://cdn.example/story-transformed.jpg");
                    assertThat(response.pageNumber()).isZero();
                    assertThat(response.totalElements()).isEqualTo(1L);
                })
                .verifyComplete();
    }

    @Test
    void handleStoryScanEventApprovesStorySendsSseAndPublishesSuccessPayload() {
        StoryMediaService service = newService();
        UserStories pendingStory = story("story-1", "owner-1", "https://cdn.example/story.jpg", "PENDING_SCAN");
        pendingStory.setMusicId("music-1");
        pendingStory.setMusicUrl("https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3");
        pendingStory.setMusicStart(5000L);
        pendingStory.setMusicEnd(15000L);
        pendingStory.setPublicationId("publication-1");
        pendingStory.setPublicationOrder(1);
        pendingStory.setPublicationItemCount(2);
        Media savedMedia = Media.builder()
                .assetId("media-1")
                .resourceType("image")
                .secureUrl("https://cdn.example/story.jpg")
                .build();

        stubStoryClaim(pendingStory);
        when(mediaScanUtils.scanMedia("https://cdn.example/story.jpg", "stories/story_1"))
                .thenReturn(Mono.just(MediaScanUtils.ScanResult.approved()));
        when(mediaService.saveCloudinaryMedia("stories/story_1", "owner-1", OwnerType.STORY))
                .thenReturn(Mono.just(savedMedia));
        when(userStoriesRepository.save(any(UserStories.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(postSseService.sendToUser(eq("owner-1"), eq("story_upload_event"), anyString()))
                .thenReturn(Mono.empty());
        when(cloudinaryMediaService.transformMusicUrlIfSupported(
                "https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3",
                5000L,
                15000L
        )).thenReturn("https://res.cloudinary.com/demo/video/upload/so_5,eo_15/v1234567890/musics/song.mp3");
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> publisher = invocation.getArgument(0);
            StepVerifier.create(Flux.from(publisher))
                    .assertNext(record -> {
                        assertThat(record.topic()).isEqualTo("story_success_event");
                        assertThat(record.key()).isEqualTo("story-1");
                        JsonObject payload = GsonUtils.fromString(record.value());
                        assertThat(payload.get("storyId").getAsString()).isEqualTo("story-1");
                        assertThat(payload.get("userId").getAsString()).isEqualTo("owner-1");
                        assertThat(payload.get("mediaUrl").getAsString()).isEqualTo("https://cdn.example/story.jpg");
                        assertThat(payload.get("mediaType").getAsString()).isEqualTo("IMAGE");
                        assertThat(payload.get("musicId").getAsString()).isEqualTo("music-1");
                        assertThat(payload.get("musicUrl").getAsString()).isEqualTo("https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3");
                        assertThat(payload.get("musicStart").getAsLong()).isEqualTo(5000L);
                        assertThat(payload.get("musicEnd").getAsLong()).isEqualTo(15000L);
                        assertThat(payload.get("publicationId").getAsString()).isEqualTo("publication-1");
                        assertThat(payload.get("publicationOrder").getAsInt()).isEqualTo(1);
                        assertThat(payload.get("publicationItemCount").getAsInt()).isEqualTo(2);
                        assertThat(payload.get("musicTransformedUrl").getAsString())
                                .isEqualTo("https://res.cloudinary.com/demo/video/upload/so_5,eo_15/v1234567890/musics/song.mp3");
                        assertThat(payload.get("mediaId").getAsString()).isEqualTo("media-1");
                    })
                    .verifyComplete();
            return Flux.empty();
        });

        service.handleStoryScanEvent(storyScanPayload()).join();

        ArgumentCaptor<UserStories> storyCaptor = ArgumentCaptor.forClass(UserStories.class);
        verify(userStoriesRepository).save(storyCaptor.capture());
        assertThat(storyCaptor.getValue().getStatus()).isEqualTo("APPROVED");
        assertThat(storyCaptor.getValue().getMediaType()).isEqualTo("IMAGE");

        ArgumentCaptor<String> ssePayload = ArgumentCaptor.forClass(String.class);
        verify(postSseService).sendToUser(eq("owner-1"), eq("story_upload_event"), ssePayload.capture());
        JsonObject sse = GsonUtils.fromString(ssePayload.getValue());
        assertThat(sse.get("entityId").getAsString()).isEqualTo("story-1");
        assertThat(sse.get("ownerType").getAsString()).isEqualTo("STORY");
        assertThat(sse.get("result").getAsString()).isEqualTo("APPROVED");
        assertThat(sse.get("musicTransformedUrl").getAsString())
                .isEqualTo("https://res.cloudinary.com/demo/video/upload/so_5,eo_15/v1234567890/musics/song.mp3");
    }

    @Test
    void handleStoryScanEventRejectsStoryDeletesCloudinaryMediaAndSendsFailureSse() {
        StoryMediaService service = newService();
        UserStories pendingStory = story("story-1", "owner-1", "https://cdn.example/story.jpg", "PENDING_SCAN");
        stubStoryClaim(pendingStory);

        when(mediaScanUtils.scanMedia("https://cdn.example/story.jpg", "stories/story_1"))
                .thenReturn(Mono.just(MediaScanUtils.ScanResult.rejected()));
        when(userStoriesRepository.save(any(UserStories.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(cloudinaryMediaService.deleteAsset("stories/story_1")).thenReturn(Mono.empty());
        when(postSseService.sendToUser(eq("owner-1"), eq("story_upload_event"), anyString()))
                .thenReturn(Mono.empty());
        when(userAuditService.save(any(AuditLogs.class))).thenReturn(Mono.empty());

        service.handleStoryScanEvent(storyScanPayload()).join();

        ArgumentCaptor<UserStories> storyCaptor = ArgumentCaptor.forClass(UserStories.class);
        verify(userStoriesRepository).save(storyCaptor.capture());
        assertThat(storyCaptor.getValue().getStatus()).isEqualTo("REJECTED");
        verify(cloudinaryMediaService).deleteAsset("stories/story_1");
        verify(userAuditService).save(any(AuditLogs.class));
        ArgumentCaptor<String> ssePayload = ArgumentCaptor.forClass(String.class);
        verify(postSseService).sendToUser(eq("owner-1"), eq("story_upload_event"), ssePayload.capture());
        JsonObject sse = GsonUtils.fromString(ssePayload.getValue());
        assertThat(sse.get("entityId").getAsString()).isEqualTo("story-1");
        assertThat(sse.get("result").getAsString()).isEqualTo("REJECTED");
        assertThat(sse.get("message").getAsString()).isEqualTo("Story rejected due to invalid media");
        verify(kafkaSender, never()).send(any(Publisher.class));
    }

    private StoryMediaService newService() {
        return new StoryMediaService(
                userDetailsRepository,
                userStoriesRepository,
                mediaService,
                cloudinaryMediaService,
                postSseService,
                kafkaSender,
                r2dbcEntityTemplate,
                mediaScanUtils,
                userAuditService,
                new StoryMusicSegmentPolicy()
        );
    }

    private void stubStoryInsert() {
        ReactiveInsertOperation.ReactiveInsert<UserStories> insertSpec = org.mockito.Mockito.mock(ReactiveInsertOperation.ReactiveInsert.class);
        when(r2dbcEntityTemplate.insert(UserStories.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(UserStories.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
    }

    private void stubStoryViewSelect(StoryView storyView) {
        ReactiveSelectOperation.ReactiveSelect<StoryView> selectSpec = org.mockito.Mockito.mock(ReactiveSelectOperation.ReactiveSelect.class);
        ReactiveSelectOperation.TerminatingSelect<StoryView> terminatingSelect = org.mockito.Mockito.mock(ReactiveSelectOperation.TerminatingSelect.class);
        when(r2dbcEntityTemplate.select(StoryView.class)).thenReturn(selectSpec);
        when(selectSpec.matching(any())).thenReturn(terminatingSelect);
        when(terminatingSelect.all()).thenReturn(Flux.just(storyView));
    }

    private void stubStoryClaim(UserStories pendingStory) {
        DatabaseClient databaseClient = org.mockito.Mockito.mock(DatabaseClient.class);
        DatabaseClient.GenericExecuteSpec executeSpec = org.mockito.Mockito.mock(DatabaseClient.GenericExecuteSpec.class);
        FetchSpec<Map<String, Object>> fetchSpec = org.mockito.Mockito.mock(FetchSpec.class);
        when(r2dbcEntityTemplate.getDatabaseClient()).thenReturn(databaseClient);
        when(databaseClient.sql(anyString())).thenReturn(executeSpec);
        when(executeSpec.bind(anyString(), any())).thenReturn(executeSpec);
        when(executeSpec.fetch()).thenReturn(fetchSpec);
        when(fetchSpec.rowsUpdated()).thenReturn(Mono.just(1L));
        when(userStoriesRepository.findById(pendingStory.getId())).thenReturn(Mono.just(pendingStory));
    }

    private String storyScanPayload() {
        JsonObject payload = new JsonObject();
        payload.addProperty("storyId", "story-1");
        payload.addProperty("userId", "owner-1");
        payload.addProperty("mediaUrl", "https://cdn.example/story.jpg");
        payload.addProperty("publicId", "stories/story_1");
        return payload.toString();
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
