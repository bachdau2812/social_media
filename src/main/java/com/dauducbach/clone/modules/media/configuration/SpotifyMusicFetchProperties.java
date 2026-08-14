package com.dauducbach.clone.modules.media.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "music.spotify")
public class SpotifyMusicFetchProperties {
    private String pythonCommand = "python";
    private String ffprobeCommand = "ffprobe";
    private String tempRoot = "";
    private Duration processTimeout = Duration.ofMinutes(5);
    private Duration lockTtl = Duration.ofMinutes(10);
    private int maxConcurrentFetches = 2;
    private int maxQueuedFetches = 50;

    public Path resolvedTempRoot() {
        if (tempRoot == null || tempRoot.isBlank()) {
            return Path.of(System.getProperty("java.io.tmpdir"), "social-media-music-fetch")
                    .toAbsolutePath()
                    .normalize();
        }
        return Path.of(tempRoot).toAbsolutePath().normalize();
    }
}
