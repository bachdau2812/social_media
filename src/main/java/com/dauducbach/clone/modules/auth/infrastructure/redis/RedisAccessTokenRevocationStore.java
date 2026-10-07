package com.dauducbach.clone.modules.auth.infrastructure.redis;

import com.dauducbach.clone.modules.auth.sessions.AccessTokenRevocationStore;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class RedisAccessTokenRevocationStore implements AccessTokenRevocationStore {
    private static final String ACCESS_TOKEN_KEY_PREFIX = "logout:";
    private static final String REVOKED_USER_KEY_PREFIX = "revoked_user:";
    private static final String REVOKED_VALUE = "REVOKED";

    private final ReactiveRedisTemplate<String, Object> redisTemplate;

    @Override
    public Mono<Boolean> revoke(String accessToken, Duration ttl) {
        return redisTemplate.opsForValue().set(ACCESS_TOKEN_KEY_PREFIX + accessToken, REVOKED_VALUE, ttl);
    }

    @Override
    public Mono<Boolean> isRevoked(String accessToken) {
        return redisTemplate.hasKey(ACCESS_TOKEN_KEY_PREFIX + accessToken);
    }

    @Override
    public Mono<Boolean> isUserRevoked(String userId) {
        return redisTemplate.hasKey(REVOKED_USER_KEY_PREFIX + userId);
    }
}
