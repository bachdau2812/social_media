package com.dauducbach.clone.modules.post.infrastructure.redis;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedisPostNotificationMuteStoreTest {
    @SuppressWarnings("unchecked")
    private final ReactiveRedisTemplate<String, String> redis = mock(ReactiveRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ReactiveValueOperations<String, String> values = mock(ReactiveValueOperations.class);
    private final RedisPostNotificationMuteStore store = new RedisPostNotificationMuteStore(redis);

    @Test
    void storesTheExistingMutedPreferenceForSixtyDays() {
        String key = PostNotificationCacheKeys.mutedPostNotification("post-1", "user-1");
        when(redis.opsForValue()).thenReturn(values);
        when(values.set(key, "true", Duration.ofDays(60))).thenReturn(Mono.just(true));

        StepVerifier.create(store.mute("post-1", "user-1")).expectNext(true).verifyComplete();

        verify(values).set(key, "true", Duration.ofDays(60));
    }

    @Test
    void readsTheExistingMutedPreferenceThroughThePostQueryContract() {
        String key = PostNotificationCacheKeys.mutedPostNotification("post-1", "user-1");
        when(redis.hasKey(key)).thenReturn(Mono.just(true));

        StepVerifier.create(store.isMuted("post-1", "user-1")).expectNext(true).verifyComplete();

        verify(redis).hasKey(key);
    }
}
