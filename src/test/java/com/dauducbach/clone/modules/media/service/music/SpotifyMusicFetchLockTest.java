package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SpotifyMusicFetchLockTest {
    private static final String TRACK_ID = "1Gqm6KaobG2A1mFVjGnJsS";
    private static final String KEY = "music:fetch:lock:" + TRACK_ID;

    @Test
    void acquiresWithSetIfAbsentAndConfiguredTtl() {
        ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ReactiveValueOperations<String, String> values = mock(ReactiveValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(KEY, "token-1", Duration.ofMinutes(10)))
                .thenReturn(Mono.just(true));

        StepVerifier.create(new SpotifyMusicFetchLock(redis, properties())
                        .tryAcquire(TRACK_ID, "token-1"))
                .expectNext(true)
                .verifyComplete();

        verify(values).setIfAbsent(KEY, "token-1", Duration.ofMinutes(10));
    }

    @Test
    void releasesOnlyThroughAtomicCompareAndDeleteScript() {
        ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);
        when(redis.execute(
                any(RedisScript.class),
                eq(List.of(KEY)),
                eq(List.of("token-1"))))
                .thenReturn(Flux.just(1L));

        StepVerifier.create(new SpotifyMusicFetchLock(redis, properties())
                        .release(TRACK_ID, "token-1"))
                .expectNext(true)
                .verifyComplete();

        verify(redis).execute(
                any(RedisScript.class),
                eq(List.of(KEY)),
                eq(List.of("token-1")));
    }

    @Test
    void mismatchedTokenDoesNotDeleteTheLock() {
        ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);
        when(redis.execute(
                any(RedisScript.class),
                eq(List.of(KEY)),
                eq(List.of("wrong-token"))))
                .thenReturn(Flux.just(0L));

        StepVerifier.create(new SpotifyMusicFetchLock(redis, properties())
                        .release(TRACK_ID, "wrong-token"))
                .expectNext(false)
                .verifyComplete();
    }

    private SpotifyMusicFetchProperties properties() {
        SpotifyMusicFetchProperties properties = new SpotifyMusicFetchProperties();
        properties.setLockTtl(Duration.ofMinutes(10));
        return properties;
    }
}
