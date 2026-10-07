package com.dauducbach.clone.modules.post.comments.infrastructure.redis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisCommentCountCacheTest {
    @Mock
    ReactiveRedisTemplate<String, String> redis;
    @Mock
    ReactiveValueOperations<String, String> values;

    @Test
    void returnsCachedCountWithoutLoadingTheStore() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.get("post_comment_count:post-1")).thenReturn(Mono.just("8"));
        AtomicBoolean loaded = new AtomicBoolean();
        RedisCommentCountCache cache = new RedisCommentCountCache(redis);

        StepVerifier.create(cache.getPostCount("post-1", () -> {
                    loaded.set(true);
                    return Mono.just(3L);
                }))
                .expectNext(8L)
                .verifyComplete();

        org.assertj.core.api.Assertions.assertThat(loaded).isFalse();
    }

    @Test
    void loadsAndCachesMissUnderTheExistingLockAndTtl() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.get("post_comment_count:post-1")).thenReturn(Mono.empty(), Mono.empty());
        when(values.setIfAbsent(eq("post_comment_count_lock:post-1"), anyString(), eq(Duration.ofSeconds(5))))
                .thenReturn(Mono.just(true));
        when(values.set("post_comment_count:post-1", "4", Duration.ofHours(24))).thenReturn(Mono.just(true));
        when(values.get("post_comment_count_lock:post-1")).thenReturn(Mono.just("expired-or-replaced"));
        RedisCommentCountCache cache = new RedisCommentCountCache(redis);

        StepVerifier.create(cache.getPostCount("post-1", () -> Mono.just(4L)))
                .expectNext(4L)
                .verifyComplete();

        verify(values).set("post_comment_count:post-1", "4", Duration.ofHours(24));
    }

    @Test
    void decrementClampsAtZeroAndRefreshesTheExistingTtl() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.setIfAbsent(eq("post_comment_count_lock:post-1"), anyString(), eq(Duration.ofSeconds(5))))
                .thenReturn(Mono.just(true));
        when(values.increment("post_comment_count:post-1", -1)).thenReturn(Mono.just(-1L));
        when(values.set("post_comment_count:post-1", "0", Duration.ofHours(24))).thenReturn(Mono.just(true));
        when(values.get("post_comment_count_lock:post-1")).thenReturn(Mono.just("expired-or-replaced"));
        RedisCommentCountCache cache = new RedisCommentCountCache(redis);

        StepVerifier.create(cache.adjustPostCount("post-1", -1)).verifyComplete();

        verify(values).set("post_comment_count:post-1", "0", Duration.ofHours(24));
    }
}
