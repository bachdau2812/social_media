package com.dauducbach.clone.modules.media.infrastructure.persistence;

import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.music.fetch.MusicTrack;
import com.dauducbach.clone.modules.media.music.fetch.MusicAudioUpload;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import com.dauducbach.clone.modules.media.service.MediaService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings({"unchecked", "rawtypes"})
class MusicFetchStoreAdapterTest {
    @Mock MusicsRepository musicsRepository;
    @Mock MediaService mediaService;
    @Mock TransactionalOperator transactionalOperator;

    @Test
    void savesTrackAndMediaMetadataInsideOneTransaction() {
        MusicTrack track = MusicTrack.builder()
                .id("track-1")
                .displayName("Song")
                .songUrl("https://media.test/song.flac")
                .fetched(true)
                .build();
        MusicAudioUpload upload = new MusicAudioUpload(
                "asset-1", "music/track-1", 0, 0, "flac", "video", 100,
                "http://media.test/song.flac", "https://media.test/song.flac", "v1", "version-1");
        MediaAudioUploadResult mediaUpload = new MediaAudioUploadResult(
                "asset-1", "music/track-1", 0, 0, "flac", "video", 100,
                "http://media.test/song.flac", "https://media.test/song.flac", "v1", "version-1");
        when(mediaService.saveFetchedMusicMedia("track-1", "Song", mediaUpload))
                .thenReturn(Mono.just(Media.builder().assetId("asset-1").build()));
        when(musicsRepository.save(any(Musics.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(transactionalOperator.transactional(any(Mono.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        StepVerifier.create(new MusicFetchStoreAdapter(
                        musicsRepository, mediaService, transactionalOperator)
                        .saveFetched(track, upload))
                .expectNext(track)
                .verifyComplete();

        verify(mediaService).saveFetchedMusicMedia("track-1", "Song", mediaUpload);
        verify(musicsRepository).save(any(Musics.class));
        verify(transactionalOperator).transactional(any(Mono.class));
    }

    @Test
    void mapsStoredTracksWithoutLeakingPersistenceEntity() {
        when(musicsRepository.findById("track-1")).thenReturn(Mono.just(Musics.builder()
                .id("track-1")
                .displayName("Song")
                .fetched(false)
                .build()));

        StepVerifier.create(new MusicFetchStoreAdapter(
                        musicsRepository, mediaService, transactionalOperator)
                        .findById("track-1"))
                .expectNext(MusicTrack.builder()
                        .id("track-1")
                        .displayName("Song")
                        .fetched(false)
                        .build())
                .verifyComplete();
    }
}
