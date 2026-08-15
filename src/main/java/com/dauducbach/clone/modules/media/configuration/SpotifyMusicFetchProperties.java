package com.dauducbach.clone.modules.media.configuration;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;
import java.time.Duration;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "music.spotify")
public class SpotifyMusicFetchProperties {
    @NotBlank
    private String serviceBaseUrl = "http://127.0.0.1:8000";

    @NotNull
    private Duration serviceTimeout = Duration.ofMinutes(6);

    @NotNull
    private DataSize artifactMaxSize = DataSize.ofMegabytes(100);

    private String tempRoot = "";

    @NotNull
    private Duration lockTtl = Duration.ofMinutes(10);

    @Min(1)
    private int maxConcurrentFetches = 2;

    @Min(1)
    private int maxQueuedFetches = 50;

    @AssertTrue(message = "music.spotify.artifact-max-size must be greater than 0")
    public boolean isArtifactMaxSizePositive() {
        return artifactMaxSize != null && artifactMaxSize.toBytes() > 0;
    }

    @AssertTrue(message = "music.spotify.service-timeout must be at least 1 second")
    public boolean isServiceTimeoutAtLeastOneSecond() {
        return isAtLeastOneSecond(serviceTimeout);
    }

    @AssertTrue(message = "music.spotify.lock-ttl must be at least 1 second")
    public boolean isLockTtlAtLeastOneSecond() {
        return isAtLeastOneSecond(lockTtl);
    }

    public Path resolvedTempRoot() {
        if (tempRoot == null || tempRoot.isBlank()) {
            return Path.of(System.getProperty("java.io.tmpdir"), "social-media-music-fetch")
                    .toAbsolutePath()
                    .normalize();
        }
        return Path.of(tempRoot).toAbsolutePath().normalize();
    }

    private static boolean isAtLeastOneSecond(Duration duration) {
        return duration != null && duration.compareTo(Duration.ofSeconds(1)) >= 0;
    }

}
