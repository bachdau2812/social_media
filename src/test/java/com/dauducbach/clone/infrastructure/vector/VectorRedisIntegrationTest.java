package com.dauducbach.clone.infrastructure.vector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

/** Opt in against a disposable LOCAL Redis only. Never loads application configuration. */
@EnabledIfEnvironmentVariable(named = "VECTOR_TEST_REDIS_PORT", matches = "[0-9]+")
class VectorRedisIntegrationTest {
    @Test void realLuaFencesRenewReleaseAndIdempotentCommitAcrossExpiry() {
        var factory = new LettuceConnectionFactory("127.0.0.1", Integer.parseInt(System.getenv("VECTOR_TEST_REDIS_PORT")));
        factory.afterPropertiesSet(); factory.start();
        var redis = new ReactiveStringRedisTemplate(factory); var state = new VectorRedisState(redis);
        String user = "vector-it-" + UUID.randomUUID();
        var old = new VectorLease(user, "old"); var next = new VectorLease(user, "next");
        try {
            assertThat(state.acquire(old, Duration.ofMillis(100)).block()).isTrue();
            assertThat(state.acquire(next, Duration.ofSeconds(2)).block()).isFalse();
            Mono.delay(Duration.ofMillis(180)).block();
            assertThat(state.acquire(next, Duration.ofSeconds(2)).block()).isTrue();
            assertThat(state.renew(old, Duration.ofSeconds(10)).block()).isFalse();
            state.release(old).block(); assertThat(state.isOwner(next).block()).isTrue();
            StepVerifier.create(state.commit(old, "stale")).expectError(UserVectorCoordinator.LeaseLostException.class).verify();
            assertThat(state.renew(next, Duration.ofSeconds(3)).block()).isTrue();
            assertThat(redis.getExpire(VectorCacheKeys.lock(user)).block()).isGreaterThan(Duration.ofSeconds(1));
            assertThat(state.commit(next, "one").block()).isEqualTo(1);
            assertThat(state.commit(next, "one").block()).isEqualTo(1);
            assertThat(state.commit(next, "two").block()).isEqualTo(2);
            assertThat(redis.opsForValue().get(VectorCacheKeys.dirty(user)).block()).isEqualTo("2");
            state.release(next).block(); assertThat(state.isOwner(next).block()).isFalse();
        } finally {
            redis.delete(VectorCacheKeys.lock(user), VectorCacheKeys.version(user), VectorCacheKeys.dirty(user),
                    VectorCacheKeys.operation(user)).block(); factory.destroy();
        }
    }
}
