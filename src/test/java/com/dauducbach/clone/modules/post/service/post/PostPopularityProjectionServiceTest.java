package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.post.dto.event.PostPopularityUpdate;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class PostPopularityProjectionServiceTest {
    private final Instant now = Instant.parse("2026-10-06T01:00:00Z");
    private final PostPopularityProperties config = new PostPopularityProperties();
    private final ReactiveRedisTemplate<String, String> redis = mock(ReactiveRedisTemplate.class);
    private final PostPopularityProjectionService service = new PostPopularityProjectionService(redis, config, Clock.fixed(now, ZoneOffset.UTC));

    @Test
    void rejectsInconsistentExpiryBeforeRedisAndIgnoresExpiredReplay() {
        StepVerifier.create(service.apply(update(now, now.plusSeconds(1))))
                .expectError(IllegalArgumentException.class).verify();
        Instant expired = now.minus(Duration.ofDays(3));
        StepVerifier.create(service.apply(update(expired, expired.plus(Duration.ofHours(48))))).verifyComplete();
        verifyNoInteractions(redis);
    }

    @Test
    void propagatesRedisFailureSoConsumerCannotAcknowledgeIt() {
        when(redis.execute(any(RedisScript.class), eq(List.of("post_popular")), any(List.class)))
                .thenReturn(Flux.error(new IllegalStateException("Redis unavailable")));
        StepVerifier.create(service.apply(update(now, now.plus(Duration.ofHours(48)))))
                .expectErrorMessage("Redis unavailable").verify();
    }

    @Test
    void acceptsAtomicNoopForDuplicateAndRejectsFutureOccurrence() {
        when(redis.execute(any(RedisScript.class), eq(List.of("post_popular")), any(List.class))).thenReturn(Flux.just(0L));
        StepVerifier.create(service.apply(update(now, now.plus(Duration.ofHours(48))))).verifyComplete();
        Instant future = now.plusSeconds(61);
        StepVerifier.create(service.apply(update(future, future.plus(Duration.ofHours(48)))))
                .expectError(IllegalArgumentException.class).verify();
    }

    private PostPopularityUpdate update(Instant since, Instant expires) {
        return new PostPopularityUpdate(1, "POPULAR_V1:p1:" + since.toEpochMilli(), "p1", since, expires, 21, "popular-v1");
    }
}
