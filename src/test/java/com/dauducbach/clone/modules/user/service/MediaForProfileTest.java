package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.service.post.PostSseService;
import com.dauducbach.clone.modules.user.dto.request.AvatarUploadRequest;
import com.dauducbach.clone.modules.user.dto.request.MusicSelectRequest;
import com.dauducbach.clone.modules.user.entity.UserMusics;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.MediaScanUtils;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class MediaForProfileTest {
    @Mock
    UserDetailsRepository userDetailsRepository;
    @Mock
    MusicsRepository musicsRepository;
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
    void rejectionWaitsForAssetCleanupBeforeSendingTheResult() {
        MediaForProfile service = newService();
        String url = "https://cdn/avatar.png";
        when(mediaScanUtils.scanMedia(url, "avatar")).thenReturn(Mono.just(MediaScanUtils.ScanResult.rejected()));
        when(cloudinaryMediaService.deleteAsset("avatar")).thenReturn(Mono.error(new IllegalStateException("cleanup failed")));
        lenient().when(postSseService.sendToUser(anyString(), anyString(), anyString())).thenReturn(Mono.empty());
        assertThatThrownBy(() -> service.handleAvatarScanEvent("{\"userId\":\"user-1\",\"avatarUrl\":\"https://cdn/avatar.png\",\"publicId\":\"avatar\"}").join())
                .hasCauseInstanceOf(IllegalStateException.class);
        verify(postSseService, never()).sendToUser(anyString(), anyString(), anyString());
    }

    @Test
    void safeAvatarIsSavedAndPublishedBeforeTheApprovalEvent() {
        MediaForProfile service = newService();
        String url = "https://res.cloudinary.com/demo/image/upload/v1/folder/avatar.png";
        when(mediaScanUtils.scanMedia(url, "folder/avatar")).thenReturn(Mono.just(MediaScanUtils.ScanResult.approved()));
        when(mediaService.saveCloudinaryMedia("folder/avatar", "user-1", OwnerType.AVATAR))
                .thenReturn(Mono.just(Media.builder().assetId("media-1").publicId("folder/avatar").secureUrl(url).build()));
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> publisher = invocation.getArgument(0);
            return Flux.from(publisher).doOnNext(record -> {
                assertThat(record.topic()).isEqualTo("avatar_update_event");
                assertThat(GsonUtils.fromString(record.value()).get("mediaId").getAsString()).isEqualTo("media-1");
            }).thenMany(Flux.empty());
        });
        when(postSseService.sendToUser(eq("user-1"), eq("avatar_upload_event"), any(String.class)))
                .thenAnswer(invocation -> {
                    JsonObject payload = GsonUtils.fromString(invocation.getArgument(2));
                    assertThat(payload.get("result").getAsString()).isEqualTo("APPROVED");
                    assertThat(payload.get("publicId").getAsString()).isEqualTo("folder/avatar");
                    return Mono.empty();
                });
        service.handleAvatarScanEvent("{\"userId\":\"user-1\",\"avatarUrl\":\"" + url + "\",\"publicId\":\"folder/avatar\"}").join();
        var order = inOrder(mediaService, kafkaSender, postSseService);
        order.verify(mediaService).saveCloudinaryMedia("folder/avatar", "user-1", OwnerType.AVATAR);
        order.verify(kafkaSender).send(any(Publisher.class));
        order.verify(postSseService).sendToUser(eq("user-1"), eq("avatar_upload_event"), any(String.class));
    }

    @Test
    void sensitiveAvatarIsDeletedWithoutReplacingTheAvatarOrNotifyingFollowers() {
        MediaForProfile service = newService();
        String url = "https://res.cloudinary.com/demo/image/upload/v1/folder/avatar.png";
        when(mediaScanUtils.scanMedia(url, "folder/avatar")).thenReturn(Mono.just(MediaScanUtils.ScanResult.rejected()));
        when(cloudinaryMediaService.deleteAsset("folder/avatar")).thenReturn(Mono.empty());
        when(postSseService.sendToUser(eq("user-1"), eq("avatar_upload_event"), any(String.class)))
                .thenAnswer(invocation -> {
                    JsonObject payload = GsonUtils.fromString(invocation.getArgument(2));
                    assertThat(payload.get("result").getAsString()).isEqualTo("REJECTED");
                    assertThat(payload.get("publicId").getAsString()).isEqualTo("folder/avatar");
                    return Mono.empty();
                });
        service.handleAvatarScanEvent("{\"userId\":\"user-1\",\"avatarUrl\":\"" + url + "\",\"publicId\":\"folder/avatar\"}").join();
        verify(cloudinaryMediaService).deleteAsset("folder/avatar");
        verify(mediaService, never()).saveCloudinaryMedia(any(), any(), any());
        verify(kafkaSender, never()).send(any(Publisher.class));
    }

    @Test
    void avatarUploadFailsWhenKafkaDoesNotAcknowledgeTheScanRequest() {
        MediaForProfile service = newService();
        when(userDetailsRepository.existsById("user-1")).thenReturn(Mono.just(true));
        SenderResult<String> result = mock(SenderResult.class);
        when(result.exception()).thenReturn(new IllegalStateException("broker unavailable"));
        when(kafkaSender.send(any(Publisher.class))).thenReturn(Flux.just(result));
        StepVerifier.create(service.uploadAvatar(new AvatarUploadRequest("user-1", "https://cdn/avatar.png")))
                .expectErrorMatches(error -> error.getCause() != null && error.getCause().getMessage().equals("broker unavailable"))
                .verify();
    }

    @Test
    void uploadAvatarPublishesScanEventWithResolvedPublicId() {
        MediaForProfile service = newService();

        when(userDetailsRepository.existsById("user-1")).thenReturn(Mono.just(true));
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> publisher = invocation.getArgument(0);
            StepVerifier.create(Flux.from(publisher))
                    .assertNext(record -> {
                        assertThat(record.topic()).isEqualTo("check_avatar_media_event");
                        assertThat(record.key()).isEqualTo("user-1");
                        JsonObject payload = GsonUtils.fromString(record.value());
                        assertThat(payload.get("publicId").getAsString()).isEqualTo("social_network_posts/avatar_1");
                    })
                    .verifyComplete();
            return Flux.empty();
        });

        StepVerifier.create(service.uploadAvatar(new AvatarUploadRequest(
                        "user-1",
                        "https://res.cloudinary.com/demo/image/upload/v1781617130/social_network_posts/avatar_1.jpg"
                )))
                .expectNextMatches(response -> response.userId().equals("user-1")
                        && response.ownerType().equals(OwnerType.AVATAR.name())
                        && response.status().equals("PENDING_SCAN"))
                .verifyComplete();
    }

    @Test
    void selectProfileMusicCreatesUserMusicAndFeatureMusicMedia() {
        MediaForProfile service = newService();
        Musics music = Musics.builder()
                .id("music-1")
                .slugName("song-slug")
                .displayName("Song")
                .songUrl("https://cdn.example/song.mp3")
                .displayImages("https://cdn.example/cover.jpg")
                .build();
        ReactiveInsertOperation.ReactiveInsert<UserMusics> insertSpec = org.mockito.Mockito.mock(ReactiveInsertOperation.ReactiveInsert.class);

        when(userDetailsRepository.existsById("user-1")).thenReturn(Mono.just(true));
        when(musicsRepository.findBySlugNameAndDisplayName("song-slug", "Song")).thenReturn(Mono.just(music));
        when(r2dbcEntityTemplate.insert(UserMusics.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(UserMusics.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(mediaService.saveFeatureMusic(eq("user-1"), eq("music-1"), eq("Song"), eq("song-slug"),
                eq("https://cdn.example/song.mp3"), eq("https://cdn.example/cover.jpg")))
                .thenReturn(Mono.just(Media.builder().assetId("media-1").build()));

        StepVerifier.create(service.selectProfileMusic(new MusicSelectRequest("user-1", "Song", "song-slug")))
                .expectNextMatches(response -> response.getUserId().equals("user-1")
                        && response.getMusicId().equals("music-1")
                        && response.getCreatedAt() != null)
                .verifyComplete();

        verify(mediaService).saveFeatureMusic("user-1", "music-1", "Song", "song-slug",
                "https://cdn.example/song.mp3", "https://cdn.example/cover.jpg");
    }

    private MediaForProfile newService() {
        lenient().when(userAuditService.save(any(AuditLogs.class))).thenReturn(Mono.empty());
        return new MediaForProfile(
                userDetailsRepository,
                musicsRepository,
                mediaService,
                cloudinaryMediaService,
                postSseService,
                kafkaSender,
                r2dbcEntityTemplate,
                mediaScanUtils,
                userAuditService
        );
    }
}
