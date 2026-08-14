package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import com.dauducbach.clone.testsupport.TestLogCapture;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MusicServiceLoggingTest {
    private static final String TRACK_ID = "1Gqm6KaobG2A1mFVjGnJsS";

    @Test
    void logsDelegationAndAcceptedStatus() {
        MusicsRepository repository = mock(MusicsRepository.class);
        SpotifyMusicFetchService fetchService = mock(SpotifyMusicFetchService.class);
        MusicFetchAcceptedResponse accepted = new MusicFetchAcceptedResponse(
                TRACK_ID, MusicFetchAcceptedResponse.Status.STARTED);
        when(fetchService.requestFetch(TRACK_ID, "user-1")).thenReturn(Mono.just(accepted));

        try (TestLogCapture logs = TestLogCapture.start(MusicService.class)) {
            StepVerifier.create(new MusicService(repository, fetchService)
                            .fetchSpotifyMusic(TRACK_ID, "user-1"))
                    .expectNext(accepted)
                    .verifyComplete();

            assertThat(logs.messages()).anyMatch(message -> message.contains(
                    "|MusicService|fetchSpotifyMusic|received|trackId=" + TRACK_ID));
            assertThat(logs.messages()).anyMatch(message -> message.contains(
                    "|MusicService|fetchSpotifyMusic|completed|trackId=" + TRACK_ID
                            + "|status=STARTED"));
        }
    }
}
