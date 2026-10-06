package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.post.dto.event.PostPopularityUpdate;
import com.dauducbach.clone.modules.post.dto.response.PopularPostRef;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs against forwarded Redis; touches only a disposable UUID-namespaced test key. */
@EnabledIfEnvironmentVariable(named = "POPULARITY_TEST_REDIS_PORT", matches = "[0-9]+")
class PostPopularityRedisIntegrationTest {
    @Test
    void luaPreservesLifetimeTiesAndCompositeCursorAfterAnchorDeletion() {
        var standalone = new RedisStandaloneConfiguration(System.getenv().getOrDefault("POPULARITY_TEST_REDIS_HOST", "127.0.0.1"),
                Integer.parseInt(System.getenv("POPULARITY_TEST_REDIS_PORT")));
        String password = System.getenv("POPULARITY_TEST_REDIS_PASSWORD");
        String username = System.getenv("POPULARITY_TEST_REDIS_USERNAME");
        if (username != null && !username.isBlank()) standalone.setUsername(username);
        if (password != null && !password.isBlank()) standalone.setPassword(password);
        var factory = new LettuceConnectionFactory(standalone);
        factory.afterPropertiesSet(); factory.start();
        var redis = new ReactiveStringRedisTemplate(factory);
        var config = new PostPopularityProperties();
        config.setRedisKey("test:post-popularity:" + UUID.randomUUID());
        Instant since = Instant.now().minusSeconds(10);
        var first = new PostPopularityProjectionService(redis, config, Clock.fixed(since, ZoneOffset.UTC));
        var query = new PopularPostQueryService(redis, config);
        try {
            first.apply(update("a", since)).block(Duration.ofSeconds(10));
            first.apply(update("b", since)).block(Duration.ofSeconds(10));
            var later = new PostPopularityProjectionService(redis, config, Clock.fixed(since.plusSeconds(100), ZoneOffset.UTC));
            later.apply(update("a", since.plusSeconds(100))).block(Duration.ofSeconds(10));
            assertThat(query.findPopularPosts(since.plusSeconds(200), since.plusSeconds(200), null, 40).block(Duration.ofSeconds(10)))
                    .containsExactly(new PopularPostRef("b", since.toEpochMilli()), new PopularPostRef("a", since.toEpochMilli()));
            redis.opsForZSet().remove(config.getRedisKey(), "b").block(Duration.ofSeconds(10));
            assertThat(query.findPopularPosts(since.plusSeconds(200), since.plusSeconds(200), new PopularPostRef("b", since.toEpochMilli()), 40)
                    .block(Duration.ofSeconds(10))).containsExactly(new PopularPostRef("a", since.toEpochMilli()));
            Instant expiredAt = since.plus(Duration.ofHours(48));
            assertThat(query.findPopularPosts(expiredAt, expiredAt, null, 40).block(Duration.ofSeconds(10))).isEmpty();
            later.apply(update("a", since)).block(Duration.ofSeconds(10));
            assertThat(redis.opsForZSet().score(config.getRedisKey(), "a").block(Duration.ofSeconds(10))).isEqualTo((double) since.toEpochMilli());
        } finally {
            redis.delete(config.getRedisKey()).block(Duration.ofSeconds(10)); factory.destroy();
        }
    }

    private PostPopularityUpdate update(String post, Instant time) {
        // Stream output is millisecond precision, independent of SQL timestamp precision.
        time = Instant.ofEpochMilli(time.toEpochMilli());
        return new PostPopularityUpdate(1, "POPULAR_V1:" + post + ":" + time.toEpochMilli(), post,
                time, time.plus(Duration.ofHours(48)), 21, "popular-v1");
    }
}
