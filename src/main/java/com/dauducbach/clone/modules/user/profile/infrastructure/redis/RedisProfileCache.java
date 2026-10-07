package com.dauducbach.clone.modules.user.profile.infrastructure.redis;

import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.profile.application.ProfileCache;
import com.dauducbach.clone.infrastructure.redis.RedisJsonCodec;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Repository
@RequiredArgsConstructor
public class RedisProfileCache implements ProfileCache {
    private static final String CACHE_KEY_PREFIX = "user_details_info:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    @Override
    public Mono<UserDetails> find(String userId) {
        return redisTemplate.opsForValue().get(key(userId))
                .flatMap(value -> Mono.justOrEmpty(RedisJsonCodec.deserialize(value, UserDetails.class)));
    }

    @Override
    public Mono<Void> put(UserDetails userDetails) {
        return Mono.defer(() -> {
            String json = RedisJsonCodec.serialize(userDetails);
            return json == null
                    ? Mono.empty()
                    : redisTemplate.opsForValue().set(key(userDetails.getUserId()), json, CACHE_TTL).then();
        });
    }

    @Override
    public Mono<Void> evict(String userId) {
        return redisTemplate.opsForValue().delete(key(userId)).then();
    }

    private String key(String userId) {
        return CACHE_KEY_PREFIX + userId;
    }
}
