package com.dauducbach.clone.modules.media.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

@Configuration
@EnableConfigurationProperties(SpotifyMusicFetchProperties.class)
public class SpotifyMusicFetchConfiguration {
    @Bean(name = "spotifyMusicFetchScheduler", destroyMethod = "dispose")
    public Scheduler spotifyMusicFetchScheduler(SpotifyMusicFetchProperties properties) {
        if (properties.getMaxConcurrentFetches() < 1) {
            throw new IllegalArgumentException("music.spotify.max-concurrent-fetches must be greater than 0");
        }
        if (properties.getMaxQueuedFetches() < 1) {
            throw new IllegalArgumentException("music.spotify.max-queued-fetches must be greater than 0");
        }
        return Schedulers.newBoundedElastic(
                properties.getMaxConcurrentFetches(),
                properties.getMaxQueuedFetches(),
                "spotify-music-fetch");
    }
}
