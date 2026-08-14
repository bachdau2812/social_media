package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MusicServiceTest {
    @Mock
    MusicsRepository musicsRepository;
    @Mock
    SpotifyMusicFetchService spotifyMusicFetchService;

    @Test
    void getMusicByIdReturnsSpotifyCatalogShape() {
        MusicService service = new MusicService(musicsRepository, spotifyMusicFetchService);
        Musics music = Musics.builder()
                .id("1Gqm6KaobG2A1mFVjGnJsS")
                .displayName("Ca Khuc Cuoi")
                .singleName("Wxrdie")
                .albumName("THE WXRDIES")
                .releaseYear((short) 2024)
                .duration(186L)
                .fetched(false)
                .build();
        when(musicsRepository.findById(music.getId())).thenReturn(Mono.just(music));

        StepVerifier.create(service.getMusicById(music.getId()))
                .assertNext(result -> {
                    assertThat(result.getAlbumName()).isEqualTo("THE WXRDIES");
                    assertThat(result.getReleaseYear()).isEqualTo((short) 2024);
                    assertThat(result.getFetched()).isFalse();
                    assertThat(result.getSongUrl()).isNull();
                })
                .verifyComplete();
    }

    @Test
    void getMusicsSearchesByKeywordAndCategory() {
        MusicService service = new MusicService(musicsRepository, spotifyMusicFetchService);
        Musics music = Musics.builder()
                .id("2plbrEY59IikOBgBGLjaoe")
                .displayName("Song")
                .slugName("song")
                .category("pop")
                .fetched(false)
                .build();

        when(musicsRepository.countSearchByCategory("song%", "pop")).thenReturn(Mono.just(1L));
        when(musicsRepository.searchByCategory(
                org.mockito.ArgumentMatchers.eq("song%"),
                org.mockito.ArgumentMatchers.eq("pop"),
                any(Pageable.class))).thenReturn(Flux.just(music));

        StepVerifier.create(service.getMusics(0, 20, " song ", " pop "))
                .assertNext(page -> {
                    assertThat(page.content()).containsExactly(music);
                    assertThat(page.pageNumber()).isZero();
                    assertThat(page.totalElements()).isEqualTo(1L);
                    assertThat(page.totalPages()).isEqualTo(1);
                })
                .verifyComplete();
    }

    @Test
    void delegatesSpotifyFetchWithAuthenticatedUserId() {
        MusicService service = new MusicService(musicsRepository, spotifyMusicFetchService);
        MusicFetchAcceptedResponse accepted = new MusicFetchAcceptedResponse(
                "1Gqm6KaobG2A1mFVjGnJsS",
                MusicFetchAcceptedResponse.Status.STARTED);
        when(spotifyMusicFetchService.requestFetch(
                "1Gqm6KaobG2A1mFVjGnJsS",
                "user-1")).thenReturn(Mono.just(accepted));

        StepVerifier.create(service.fetchSpotifyMusic(
                        "1Gqm6KaobG2A1mFVjGnJsS",
                        "user-1"))
                .expectNext(accepted)
                .verifyComplete();
    }
}
