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
        when(runner.run(any(CliCommandRequest.class))).thenReturn(Mono.just(new CliCommandResult(0, "done")));

        StepVerifier.create(client().download(TRACK_ID, tempDirectory))
                .expectNext(flac)
                .verifyComplete();

        ArgumentCaptor<CliCommandRequest> request = ArgumentCaptor.forClass(CliCommandRequest.class);
        org.mockito.Mockito.verify(runner).run(request.capture());
        assertThat(request.getValue().label()).isEqualTo("SpotiFLAC");
        assertThat(request.getValue().trackId()).isEqualTo(TRACK_ID);
        assertThat(request.getValue().jobId()).isEqualTo(tempDirectory.getFileName().toString());
        assertThat(request.getValue().logOutput()).isTrue();
        assertThat(request.getValue().command()).containsExactly(
                "python",
                tempDirectory.resolve("spotiflac-native-no-browser.py").toString(),
                "https://open.spotify.com/track/" + TRACK_ID,
                tempDirectory.toString(),
                "--service", "deezer", "qobuz", "tidal",
                "--no-extensions-fallback",
                "--quality", "LOSSLESS",
                "--retries", "2",
                "--timeout", "180",
                "--verbose");
        assertThat(Files.readString(tempDirectory.resolve("spotiflac-native-no-browser.py")))
                .contains("SUPPORTED_SPOTIFLAC_VERSION = \"1.6.0\"")
                .contains("_COMMUNITY_APIS.clear()")
                .contains("Browser verification is disabled")
                .contains("main()");
    }

    @Test
    void failsWhenNoFlacWasDownloaded() {
        when(runner.run(any(CliCommandRequest.class))).thenReturn(Mono.just(new CliCommandResult(
                0,
                "Timeout reached for track. Provider requires grant authentication.")));

        StepVerifier.create(client().download(TRACK_ID, tempDirectory))
                .expectErrorMatches(error -> error.getMessage()
                        .contains("provider timeout or authentication required"))
                .verify();
    }

    @Test
    void nonZeroExitUsesSafeDiagnosticWithoutExposingCliOutput() {
        when(runner.run(any(CliCommandRequest.class))).thenReturn(Mono.just(new CliCommandResult(
                7,
                "Access token acquired: very-secret-token. Timeout reached.")));

        StepVerifier.create(client().download(TRACK_ID, tempDirectory))
                .expectErrorMatches(error -> error.getMessage().contains("exit code 7")
                        && error.getMessage().contains("provider timeout or authentication required")
                        && !error.getMessage().contains("very-secret-token"))
                .verify();
    }

    @Test
    void rejectsAnEmptyFlacFile() throws Exception {
        Files.createFile(tempDirectory.resolve("empty.flac"));
        when(runner.run(any(CliCommandRequest.class))).thenReturn(Mono.just(new CliCommandResult(0, "done")));

        StepVerifier.create(client().download(TRACK_ID, tempDirectory))
                .expectErrorMatches(error -> error.getMessage().contains("empty FLAC"))
                .verify();
    }

    @Test
    void failsWhenMultipleFlacsWereDownloaded() throws Exception {
        Files.writeString(tempDirectory.resolve("one.flac"), "one");
        Files.writeString(tempDirectory.resolve("two.FLAC"), "two");
        when(runner.run(any(CliCommandRequest.class))).thenReturn(Mono.just(new CliCommandResult(0, "done")));

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
        properties.setPythonCommand("python");
        properties.setProcessTimeout(Duration.ofMinutes(5));
        return new SpotiFlacCliClient(runner, properties);
    }
}
