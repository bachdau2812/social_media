package com.dauducbach.clone.modules.user.profile.application;

import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MusicCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicTrackView;
import com.dauducbach.clone.modules.user.dto.request.MusicSelectRequest;
import com.dauducbach.clone.modules.user.entity.UserMusics;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProfileMusicUseCaseTest {
    @Mock UserIdentityQuery userIdentityQuery;
    @Mock MusicCatalog musicCatalog;
    @Mock ProfileMusicStore profileMusicStore;
    @Mock MediaAssets mediaAssets;
    @Mock AuditRecorder auditRecorder;

    @Test
    void selectingProfileMusicPersistsSelectionRegistersMediaAndAudits() {
        ProfileMusicUseCase useCase = new ProfileMusicUseCase(
                userIdentityQuery, musicCatalog, profileMusicStore, mediaAssets, auditRecorder);
        MusicTrackView music = new MusicTrackView(
                "music-1", "song-slug", "Song", null, "https://cdn.example/cover.jpg",
                null, "https://cdn.example/song.mp3", null, null, null, null, null, null);
        when(userIdentityQuery.exists("user-1")).thenReturn(Mono.just(true));
        when(musicCatalog.findBySlugNameAndDisplayName("song-slug", "Song")).thenReturn(Mono.just(music));
        when(profileMusicStore.insert(any(UserMusics.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(mediaAssets.registerFeatureMusic(
                "user-1", "music-1", "Song", "song-slug", "https://cdn.example/song.mp3", "https://cdn.example/cover.jpg"))
                .thenReturn(Mono.empty());
        when(auditRecorder.record(any())).thenReturn(Mono.empty());

        StepVerifier.create(useCase.selectProfileMusic(new MusicSelectRequest("user-1", "Song", "song-slug")))
                .expectNextMatches(selection -> selection.getUserId().equals("user-1")
                        && selection.getMusicId().equals("music-1")
                        && selection.getCreatedAt() != null)
                .verifyComplete();

        verify(mediaAssets).registerFeatureMusic(
                "user-1", "music-1", "Song", "song-slug", "https://cdn.example/song.mp3", "https://cdn.example/cover.jpg");
        verify(auditRecorder).record(any());
    }

    @Test
    void fallsBackToSlugOnlyLookupWhenTrackDisplayNameChanged() {
        ProfileMusicUseCase useCase = new ProfileMusicUseCase(
                userIdentityQuery, musicCatalog, profileMusicStore, mediaAssets, auditRecorder);
        MusicTrackView music = new MusicTrackView(
                "music-1", "song-slug", "Song", null, null, null,
                "https://cdn.example/song.mp3", null, null, null, null, null, null);
        when(userIdentityQuery.exists("user-1")).thenReturn(Mono.just(true));
        when(musicCatalog.findBySlugNameAndDisplayName("song-slug", "Old Song Name"))
                .thenReturn(Mono.empty());
        when(musicCatalog.findBySlugName("song-slug")).thenReturn(Mono.just(music));
        when(profileMusicStore.insert(any(UserMusics.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(mediaAssets.registerFeatureMusic(any(), any(), any(), any(), any(), eq(null)))
                .thenReturn(Mono.empty());
        when(auditRecorder.record(any())).thenReturn(Mono.empty());

        StepVerifier.create(useCase.selectProfileMusic(new MusicSelectRequest("user-1", "Old Song Name", "song-slug")))
                .expectNextMatches(selection -> selection.getMusicId().equals("music-1"))
                .verifyComplete();

        verify(musicCatalog).findBySlugName("song-slug");
    }
}
