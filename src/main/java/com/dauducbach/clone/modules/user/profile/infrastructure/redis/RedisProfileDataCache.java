package com.dauducbach.clone.modules.user.profile.infrastructure.redis;

import com.dauducbach.clone.modules.user.profile.application.ProfileDataCache;
import com.dauducbach.clone.infrastructure.redis.RedisJsonCodec;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor
public class RedisProfileDataCache implements ProfileDataCache {
    private final ReactiveRedisTemplate<String, String> redisTemplate;

    @Override
    public <T> Mono<T> find(String key, Class<T> recordType) {
        return redisTemplate.opsForValue().get(key)
                .flatMap(value -> Mono.justOrEmpty(RedisJsonCodec.deserialize(value, recordType)))
                .onErrorResume(error -> {
                    log.warn("Profile cache read failed; treat as miss|key={}|error={}", key, error.getMessage());
                    return Mono.empty();
                });
    }

    @Override
    public <T> Mono<List<T>> findList(String key, Class<T> recordType) {
        return redisTemplate.opsForValue().get(key)
                .map(value -> RedisJsonCodec.deserializeList(value, recordType))
                .onErrorResume(error -> {
                    log.warn("Profile cache list read failed; treat as miss|key={}|error={}", key, error.getMessage());
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> put(String key, Object value, Duration ttl) {
        return Mono.defer(() -> {
            String serialized = RedisJsonCodec.serialize(value);
            return serialized == null
                    ? Mono.empty()
                    : redisTemplate.opsForValue().set(key, serialized, ttl).then();
        }).onErrorResume(error -> {
            log.warn("Profile cache write failed; continue|key={}|error={}", key, error.getMessage());
            return Mono.empty();
        });
    }

    @Override
    public Mono<Void> evict(String key) {
        return redisTemplate.opsForValue().delete(key)
                .then()
                .onErrorResume(error -> {
                    log.warn("Profile cache eviction failed; continue|key={}|error={}", key, error.getMessage());
                    return Mono.empty();
                });
    }
}
