package com.dauducbach.clone.modules.media.controller;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.service.music.MusicService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MusicsControllerTest {
    private static final String TRACK_ID = "1Gqm6KaobG2A1mFVjGnJsS";

    @Test
    void returnsAcceptedForEveryFetchStatusAndUsesAuthenticationName() {
        for (MusicFetchAcceptedResponse.Status status
                : MusicFetchAcceptedResponse.Status.values()) {
            MusicService service = mock(MusicService.class);
            Authentication authentication = mock(Authentication.class);
            MusicFetchAcceptedResponse result =
                    new MusicFetchAcceptedResponse(TRACK_ID, status);
            when(authentication.isAuthenticated()).thenReturn(true);
            when(authentication.getName()).thenReturn("user-1");
            when(service.fetchSpotifyMusic(TRACK_ID, "user-1"))
                    .thenReturn(Mono.just(result));

            StepVerifier.create(new MusicsController(service)
                            .fetchSpotifyMusic(TRACK_ID, authentication))
                    .assertNext(response -> {
                        assertThat(response.getStatusCode())
                                .isEqualTo(HttpStatus.ACCEPTED);
                        assertThat(response.getBody()).isNotNull();
                        assertThat(response.getBody().getResult()).isEqualTo(result);
                    })
                    .verifyComplete();
        }
    }

    @Test
    void rejectsMissingAuthenticationBeforeCallingService() {
        MusicService service = mock(MusicService.class);

        StepVerifier.create(new MusicsController(service)
                        .fetchSpotifyMusic(TRACK_ID, null))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(AppException.class);
                    assertThat(((AppException) error).getErrorCode())
                            .isEqualTo(ErrorCode.AUTHENTICATION_FAILED);
                })
                .verify();
    }

    @Test
    void propagatesInvalidTrackAndMissingMusicErrors() {
        MusicService service = mock(MusicService.class);
        Authentication authentication = mock(Authentication.class);
        when(authentication.isAuthenticated()).thenReturn(true);
        when(authentication.getName()).thenReturn("user-1");
        when(service.fetchSpotifyMusic("invalid", "user-1"))
                .thenReturn(Mono.error(new IllegalArgumentException("invalid track")));
        when(service.fetchSpotifyMusic(TRACK_ID, "user-1"))
                .thenReturn(Mono.error(new AppException(ErrorCode.MUSIC_NOT_FOUND)));

        StepVerifier.create(new MusicsController(service)
                        .fetchSpotifyMusic("invalid", authentication))
                .expectError(IllegalArgumentException.class)
                .verify();
        StepVerifier.create(new MusicsController(service)
                        .fetchSpotifyMusic(TRACK_ID, authentication))
                .expectErrorMatches(error -> error instanceof AppException appError
                        && appError.getErrorCode() == ErrorCode.MUSIC_NOT_FOUND)
                .verify();
    }
}
