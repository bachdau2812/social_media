package com.dauducbach.clone.modules.media.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableConfigurationProperties(SpotifyMusicFetchProperties.class)
public class SpotifyMusicFetchConfiguration {
    @Bean(name = "spotifyMusicFetchScheduler", destroyMethod = "dispose")
    public Scheduler spotifyMusicFetchScheduler(SpotifyMusicFetchProperties properties) {
        validateLimits(properties);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                properties.getMaxConcurrentFetches(),
                properties.getMaxConcurrentFetches(),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(properties.getMaxQueuedFetches()),
                Thread.ofPlatform().name("spotify-music-fetch-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
        return Schedulers.fromExecutorService(executor);
    }

    @Bean(name = "spotifyMusicProcessScheduler", destroyMethod = "dispose")
    public Scheduler spotifyMusicProcessScheduler(SpotifyMusicFetchProperties properties) {
        validateLimits(properties);
        return Schedulers.newBoundedElastic(
                properties.getMaxConcurrentFetches(),
                properties.getMaxQueuedFetches(),
                "spotify-music-process");
    }

    private void validateLimits(SpotifyMusicFetchProperties properties) {
        if (properties.getMaxConcurrentFetches() < 1) {
            throw new IllegalArgumentException("music.spotify.max-concurrent-fetches must be greater than 0");
        }
        if (properties.getMaxQueuedFetches() < 1) {
            throw new IllegalArgumentException("music.spotify.max-queued-fetches must be greater than 0");
        }
    }
}
