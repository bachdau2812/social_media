package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Component
@RequiredArgsConstructor
public class SpotiFlacCliClient {
    private static final Pattern SPOTIFY_TRACK_ID = Pattern.compile("[A-Za-z0-9]{22}");
    private static final String SPOTIFY_TRACK_PREFIX = "https://open.spotify.com/track/";

    private final CliCommandRunner runner;
    private final SpotifyMusicFetchProperties properties;

    public Mono<Path> download(String trackId, Path jobDirectory) {
        if (trackId == null || !SPOTIFY_TRACK_ID.matcher(trackId).matches()) {
            return Mono.error(new IllegalArgumentException("Spotify trackId must contain 22 base-62 characters"));
        }
        if (jobDirectory == null) {
            return Mono.error(new IllegalArgumentException("Job directory is required"));
        }

        Path normalizedDirectory = jobDirectory.toAbsolutePath().normalize();
        List<String> command = List.of(
                properties.getSpotiflacCommand(),
                SPOTIFY_TRACK_PREFIX + trackId,
                normalizedDirectory.toString(),
                "--service", "deezer", "qobuz", "tidal",
                "--quality", "LOSSLESS",
                "--retries", "2",
                "--timeout", "180",
                "--verbose");

        return Mono.fromCallable(() -> Files.createDirectories(normalizedDirectory))
                .flatMap(ignored -> runner.run(command, properties.getProcessTimeout()))
                .flatMap(result -> result.exitCode() == 0
                        ? findOnlyFlac(normalizedDirectory, result.output())
                        : Mono.error(new IllegalStateException(
                                "SpotiFLAC failed with exit code "
                                        + result.exitCode()
                                        + ": "
                                        + failureReason(result.output()))));
    }

    private Mono<Path> findOnlyFlac(Path jobDirectory, String cliOutput) {
        return Mono.fromCallable(() -> {
            List<Path> flacFiles;
            try (Stream<Path> files = Files.walk(jobDirectory)) {
                flacFiles = files
                        .filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString()
                                .toLowerCase(Locale.ROOT)
                                .endsWith(".flac"))
                        .toList();
            }
            if (flacFiles.isEmpty()) {
                throw new IllegalStateException(
                        "SpotiFLAC completed without a FLAC file: " + failureReason(cliOutput));
            }
            if (flacFiles.size() > 1) {
                throw new IllegalStateException(
                        "SpotiFLAC must produce exactly one FLAC file, found " + flacFiles.size());
            }
            return flacFiles.getFirst();
        });
    }

    private String failureReason(String cliOutput) {
        String normalized = cliOutput == null
                ? ""
                : cliOutput.toLowerCase(Locale.ROOT);
        boolean timedOut = normalized.contains("timeout reached")
                || normalized.contains("timed out");
        boolean authenticationRequired = normalized.contains("requires grant")
                || normalized.contains("grant authentication")
                || normalized.contains("authentication required")
                || normalized.contains("incolla qui il grant");
        if (timedOut || authenticationRequired) {
            return "provider timeout or authentication required";
        }
        if (normalized.contains("unicodeencodeerror")) {
            return "Python CLI output encoding failed";
        }
        if (normalized.contains("failed") || normalized.contains("no matching track")) {
            return "all configured providers failed";
        }
        return "no configured provider produced an audio file";
    }
}
