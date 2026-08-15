package com.dauducbach.clone.modules.media.configuration;

import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Scheduler;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpotifyMusicFetchConfigurationTest {
    @Test
    void exposesOnlyTheBoundedJobScheduler() {
        assertThat(Arrays.stream(SpotifyMusicFetchConfiguration.class.getDeclaredMethods())
                        .map(method -> method.getName()))
                .contains("spotifyMusicFetchScheduler")
                .doesNotContain("spotifyMusicProcessScheduler");
    }

    @Test
    void rejectsBeforeAcceptingMoreThanTheConfiguredRunningAndQueuedJobs() throws Exception {
        SpotifyMusicFetchProperties properties = new SpotifyMusicFetchProperties();
        properties.setMaxConcurrentFetches(1);
        properties.setMaxQueuedFetches(1);
        Scheduler scheduler = new SpotifyMusicFetchConfiguration()
                .spotifyMusicFetchScheduler(properties);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        try {
            scheduler.schedule(() -> {
                started.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            scheduler.schedule(() -> { });

            assertThatThrownBy(() -> scheduler.schedule(() -> { }))
                    .isInstanceOf(RejectedExecutionException.class);
        } finally {
            release.countDown();
            scheduler.dispose();
        }
    }
}
