package com.dauducbach.clone.modules.post.infrastructure.redis;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.infrastructure.redis.RedisJsonCodec;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedisPostDetailsCacheTest {
    @SuppressWarnings("unchecked")
    private final ReactiveRedisTemplate<String, String> redis = mock(ReactiveRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ReactiveValueOperations<String, String> values = mock(ReactiveValueOperations.class);
    private final RedisPostDetailsCache cache = new RedisPostDetailsCache(redis);

    @Test
    void readsExistingSnapshotAndReturnsEmptyOnMiss() {
        PostDetails post = PostDetails.builder().postId("post-1").userId("user-1").content("hello").build();
        when(redis.opsForValue()).thenReturn(values);
        when(values.get("post:details:v3:post-1")).thenReturn(Mono.just(RedisJsonCodec.serialize(post)));
        when(values.get("post:details:v3:missing")).thenReturn(Mono.empty());

        StepVerifier.create(cache.get("post-1"))
                .assertNext(cached -> assertThat(cached.getContent()).isEqualTo("hello"))
                .verifyComplete();
        StepVerifier.create(cache.get("missing")).verifyComplete();
    }

    @Test
    void writesWithTheExistingKeyAndTwentyFourHourTtl() {
        PostDetails post = PostDetails.builder().postId("post-1").userId("user-1").content("hello").build();
        when(redis.opsForValue()).thenReturn(values);
        when(values.set("post:details:v3:post-1", RedisJsonCodec.serialize(post), Duration.ofHours(24)))
                .thenReturn(Mono.just(true));

        StepVerifier.create(cache.put(post)).expectNext(true).verifyComplete();
        verify(values).set("post:details:v3:post-1", RedisJsonCodec.serialize(post), Duration.ofHours(24));
    }

    @Test
    void evictsSingleAndBulkSnapshots() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.delete("post:details:v3:post-1")).thenReturn(Mono.just(true));
        when(values.delete("post:details:v3:post-2")).thenReturn(Mono.just(true));

        StepVerifier.create(cache.evict("post-1")).verifyComplete();
        StepVerifier.create(cache.evictAll(java.util.List.of("post-1", "post-2"))).verifyComplete();

        verify(values, times(2)).delete("post:details:v3:post-1");
        verify(values).delete("post:details:v3:post-2");
    }
}
