package com.dauducbach.clone.modules.media.publicapi;

import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import com.dauducbach.clone.modules.media.service.MusicCatalogService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MusicCatalogTest {
    @Mock
    private MusicsRepository musicsRepository;

    @Test
    void mapsPersistenceEntityToStablePublicSnapshot() {
        Musics music = Musics.builder()
                .id("track-1")
                .slugName("midnight-echo")
                .displayName("Midnight Echo")
                .descriptions("A track")
                .displayImages("image-url")
                .singleName("North Avenue")
                .songUrl("audio-url")
                .duration(220L)
                .category("pop")
                .releaseYear((short) 2025)
                .albumName("First Light")
                .fetched(true)
                .popularity(new BigDecimal("12.5"))
                .build();
        when(musicsRepository.findAllById(java.util.List.of("track-1")))
                .thenReturn(Flux.just(music));

        StepVerifier.create(new MusicCatalogService(musicsRepository).findAllByIds(java.util.List.of("track-1")))
                .expectNext(new MusicTrackView(
                        "track-1", "midnight-echo", "Midnight Echo", "A track", "image-url",
                        "North Avenue", "audio-url", 220L, "pop", (short) 2025, "First Light",
                        true, new BigDecimal("12.5")))
                .verifyComplete();
    }
}
