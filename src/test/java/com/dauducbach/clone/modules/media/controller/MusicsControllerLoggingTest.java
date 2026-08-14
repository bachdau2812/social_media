package com.dauducbach.clone.modules.media.controller;

import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.service.music.MusicService;
import com.dauducbach.clone.testsupport.TestLogCapture;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MusicsControllerLoggingTest {
    private static final String TRACK_ID = "1Gqm6KaobG2A1mFVjGnJsS";

    @Test
    void logsRejectedAndAcceptedFetchBoundaries() {
        MusicService service = mock(MusicService.class);
        MusicsController controller = new MusicsController(service);

        try (TestLogCapture logs = TestLogCapture.start(MusicsController.class)) {
            StepVerifier.create(controller.fetchSpotifyMusic(TRACK_ID, null))
                    .expectError()
                    .verify();

            Authentication authentication = mock(Authentication.class);
            MusicFetchAcceptedResponse accepted = new MusicFetchAcceptedResponse(
                    TRACK_ID, MusicFetchAcceptedResponse.Status.STARTED);
            when(authentication.isAuthenticated()).thenReturn(true);
            when(authentication.getName()).thenReturn("user-1");
            when(service.fetchSpotifyMusic(TRACK_ID, "user-1")).thenReturn(Mono.just(accepted));

            StepVerifier.create(controller.fetchSpotifyMusic(TRACK_ID, authentication))
                    .expectNextCount(1)
                    .verifyComplete();

            assertThat(logs.messages()).anyMatch(message -> message.contains(
                    "|MusicsController|fetchSpotifyMusic|rejected|trackId=" + TRACK_ID));
            assertThat(logs.messages()).anyMatch(message -> message.contains(
                    "|MusicsController|fetchSpotifyMusic|accepted|trackId=" + TRACK_ID
                            + "|status=STARTED"));
        }
    }
}
