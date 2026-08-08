package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repositoty.music.MusicsRepository;
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
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
