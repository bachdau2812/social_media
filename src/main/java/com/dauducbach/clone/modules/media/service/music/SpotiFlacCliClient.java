package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Component
@RequiredArgsConstructor
public class SpotiFlacCliClient {
    private static final Pattern SPOTIFY_TRACK_ID = Pattern.compile("[A-Za-z0-9]{22}");
    private static final String SPOTIFY_TRACK_PREFIX = "https://open.spotify.com/track/";
    private static final String NO_BROWSER_LAUNCHER_RESOURCE = "/spotiflac/native_no_browser.py";
    private static final String NO_BROWSER_LAUNCHER_FILE = "spotiflac-native-no-browser.py";

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
        Path launcher = normalizedDirectory.resolve(NO_BROWSER_LAUNCHER_FILE);
        List<String> command = List.of(
                properties.getPythonCommand(),
                launcher.toString(),
                SPOTIFY_TRACK_PREFIX + trackId,
                normalizedDirectory.toString(),
                "--service", "deezer", "qobuz", "tidal",
                "--no-extensions-fallback",
                "--quality", "LOSSLESS",
                "--retries", "2",
                "--timeout", "180",
                "--verbose");

        return Mono.fromCallable(() -> prepareNoBrowserLauncher(normalizedDirectory, launcher))
                .flatMap(ignored -> runner.run(new CliCommandRequest(
                        "SpotiFLAC",
                        trackId,
                        normalizedDirectory.getFileName().toString(),
                        command,
                        properties.getProcessTimeout(),
                        true)))
                .flatMap(result -> result.exitCode() == 0
                        ? findOnlyFlac(normalizedDirectory, result.output())
                        : Mono.error(new IllegalStateException(
                                "SpotiFLAC failed with exit code "
                                        + result.exitCode()
                                        + ": "
                                        + failureReason(result.output()))));
    }

    private Path prepareNoBrowserLauncher(Path jobDirectory, Path launcher) throws Exception {
        Files.createDirectories(jobDirectory);
        try (InputStream resource = SpotiFlacCliClient.class.getResourceAsStream(
                NO_BROWSER_LAUNCHER_RESOURCE)) {
            if (resource == null) {
                throw new IllegalStateException("SpotiFLAC no-browser launcher resource is missing");
            }
            Files.copy(resource, launcher, StandardCopyOption.REPLACE_EXISTING);
        }
        return launcher;
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
            Path flacFile = flacFiles.getFirst();
            if (Files.size(flacFile) <= 0L) {
                throw new IllegalStateException("SpotiFLAC produced an empty FLAC file");
            }
            return flacFile;
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
