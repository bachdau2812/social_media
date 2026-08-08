package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SpotiFlacCliClientTest {
    private static final String TRACK_ID = "1Gqm6KaobG2A1mFVjGnJsS";

    @Mock
    CliCommandRunner runner;

    @TempDir
    Path tempDirectory;

    @Test
    void invokesExactCommandAndReturnsTheOnlyRecursiveFlac() throws Exception {
        Path nested = Files.createDirectories(tempDirectory.resolve("deezer"));
        Path flac = Files.writeString(nested.resolve("song.flac"), "audio");
        when(runner.run(anyList(), any())).thenReturn(Mono.just(new CliCommandResult(0, "done")));

        StepVerifier.create(client().download(TRACK_ID, tempDirectory))
                .expectNext(flac)
                .verifyComplete();

        ArgumentCaptor<List<String>> command = ArgumentCaptor.forClass(List.class);
        org.mockito.Mockito.verify(runner).run(command.capture(), any(Duration.class));
        assertThat(command.getValue()).containsExactly(
                "spotiflac",
                "https://open.spotify.com/track/" + TRACK_ID,
                tempDirectory.toString(),
                "--service", "deezer", "qobuz", "tidal",
                "--quality", "LOSSLESS",
                "--retries", "2",
                "--timeout", "180",
                "--verbose");
    }

    @Test
    void failsWhenNoFlacWasDownloaded() {
        when(runner.run(anyList(), any())).thenReturn(Mono.just(new CliCommandResult(0, "done")));

        StepVerifier.create(client().download(TRACK_ID, tempDirectory))
                .expectErrorMatches(error -> error.getMessage().contains("exactly one FLAC"))
                .verify();
    }

    @Test
    void failsWhenMultipleFlacsWereDownloaded() throws Exception {
        Files.writeString(tempDirectory.resolve("one.flac"), "one");
        Files.writeString(tempDirectory.resolve("two.FLAC"), "two");
        when(runner.run(anyList(), any())).thenReturn(Mono.just(new CliCommandResult(0, "done")));

        StepVerifier.create(client().download(TRACK_ID, tempDirectory))
                .expectErrorMatches(error -> error.getMessage().contains("exactly one FLAC"))
                .verify();
    }

    @Test
    void rejectsInvalidSpotifyTrackIdBeforeStartingProcess() {
        StepVerifier.create(client().download("not-a-track-id", tempDirectory))
                .expectError(IllegalArgumentException.class)
                .verify();

        org.mockito.Mockito.verifyNoInteractions(runner);
    }

    private SpotiFlacCliClient client() {
        SpotifyMusicFetchProperties properties = new SpotifyMusicFetchProperties();
        properties.setSpotiflacCommand("spotiflac");
        properties.setProcessTimeout(Duration.ofMinutes(5));
        return new SpotiFlacCliClient(runner, properties);
    }
}
