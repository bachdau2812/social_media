package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.media.dto.music.request.JamendoMusicImportRequest;
import com.dauducbach.clone.modules.media.dto.music.request.MusicCreateRequest;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repositoty.music.MusicsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MusicServiceTest {
    @Mock
    MusicsRepository musicsRepository;
    @Mock
    MediaService mediaService;
    @Mock
    R2dbcEntityTemplate r2dbcEntityTemplate;
    @Mock
    ReactiveRedisTemplate<String, String> redisTemplate;
    @Mock
    WebClient webClient;
    @Mock
    MediaCompatibilityFacade mediaFacade;

    @Test
    void createMusicSavesManualMusicAndEvictsListCache() {
        MusicService service = newService();
        ReactiveInsertOperation.ReactiveInsert<Musics> insertSpec = org.mockito.Mockito.mock(ReactiveInsertOperation.ReactiveInsert.class);

        when(r2dbcEntityTemplate.insert(Musics.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(Musics.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(redisTemplate.delete("user:musics:list")).thenReturn(Mono.just(1L));

        StepVerifier.create(service.createMusic(new MusicCreateRequest(
                        " Bai Hat Moi ",
                        "description",
                        "https://cdn.example/cover.jpg",
                        "Singer",
                        " https://cdn.example/song.mp3 ",
                        180L,
                        "pop",
                        LocalDate.of(2026, 6, 17)
                )))
                .assertNext(saved -> {
                    assertThat(saved.getId()).isNotBlank();
                    assertThat(saved.getDisplayName()).isEqualTo("Bai Hat Moi");
                    assertThat(saved.getSlugName()).isEqualTo("bai-hat-moi");
                    assertThat(saved.getSongUrl()).isEqualTo("https://cdn.example/song.mp3");
                    assertThat(saved.getCategory()).isEqualTo("pop");
                })
                .verifyComplete();
    }

    @Test
    void importFromJamendoRejectsNonJamendoUrl() {
        MusicService service = newService();

        StepVerifier.create(service.importFromJamendo(
                        new JamendoMusicImportRequest("https://example.com/v3.0/tracks", "pop"),
                        null
                ))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(AppException.class);
                    assertThat(((AppException) error).getErrorCode()).isEqualTo(ErrorCode.MUSIC_REQUEST_INVALID);
                })
                .verify();
    }

    @Test
    void getMusicsSearchesByKeywordAndCategory() {
        MusicService service = newService();
        Musics music = Musics.builder()
                .id("music-1")
                .displayName("Song")
                .slugName("song")
                .category("pop")
                .build();

        when(musicsRepository.countSearchByCategory("song", "pop")).thenReturn(Mono.just(1L));
        when(musicsRepository.searchByCategory(org.mockito.ArgumentMatchers.eq("song"),
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

    private MusicService newService() {
        return new MusicService(
                musicsRepository,
                mediaService,
                r2dbcEntityTemplate,
                redisTemplate,
                webClient,
                mediaFacade
        );
    }
}
