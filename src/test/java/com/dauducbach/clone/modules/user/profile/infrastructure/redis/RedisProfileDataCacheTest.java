package com.dauducbach.clone.modules.user.profile.infrastructure.redis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedisProfileDataCacheTest {
    private final ReactiveRedisTemplate<String, String> redis = mock(ReactiveRedisTemplate.class);
    private final ReactiveValueOperations<String, String> values = mock(ReactiveValueOperations.class);
    private final RedisProfileDataCache cache = new RedisProfileDataCache(redis);

    @BeforeEach
    void setUp() {
        when(redis.opsForValue()).thenReturn(values);
    }

    @Test
    void readsTypedListsFromSerializedValues() {
        when(values.get("profile:jobs:user-1"))
                .thenReturn(Mono.just("[{\"id\":\"job-1\"},{\"id\":\"job-2\"}]"));

        StepVerifier.create(cache.findList("profile:jobs:user-1", CacheRecord.class))
                .expectNext(List.of(new CacheRecord("job-1"), new CacheRecord("job-2")))
                .verifyComplete();
    }

    @Test
    void writesSerializedValueWithRequestedTtl() {
        CacheRecord record = new CacheRecord("job-1");
        when(values.set("profile:job:job-1", "{\"id\":\"job-1\"}", Duration.ofHours(24)))
                .thenReturn(Mono.just(true));

        StepVerifier.create(cache.put("profile:job:job-1", record, Duration.ofHours(24)))
                .verifyComplete();

        verify(values).set("profile:job:job-1", "{\"id\":\"job-1\"}", Duration.ofHours(24));
    }

    @Test
    void cacheWriteFailureDoesNotFailTheCaller() {
        when(values.set("profile:job:job-1", "{\"id\":\"job-1\"}", Duration.ofHours(24)))
                .thenReturn(Mono.error(new IllegalStateException("cache unavailable")));

        StepVerifier.create(cache.put("profile:job:job-1", new CacheRecord("job-1"), Duration.ofHours(24)))
                .verifyComplete();
    }

    private record CacheRecord(String id) { }
}
