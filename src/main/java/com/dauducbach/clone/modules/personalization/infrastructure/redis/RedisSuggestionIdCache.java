package com.dauducbach.clone.modules.personalization.infrastructure.redis;

import com.dauducbach.clone.modules.personalization.discovery.SuggestionIdCache;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisSuggestionIdCache implements SuggestionIdCache {
    private static final String PREFIX = "user:suggestions:v1:";
    private static final Duration TTL = Duration.ofHours(36);
    private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

    private final ReactiveRedisTemplate<String, String> redis;

    @Override
    public Mono<List<String>> get(String viewerId) {
        return redis.opsForValue().get(PREFIX + viewerId)
                .map(json -> {
                    try {
                        List<String> ids = GsonUtils.getGson().fromJson(json, STRING_LIST_TYPE);
                        return ids == null ? List.<String>of() : List.copyOf(ids);
                    } catch (RuntimeException invalidCacheEntry) {
                        return List.<String>of();
                    }
                })
                .onErrorReturn(List.of())
                .defaultIfEmpty(List.of());
    }

    @Override
    public Mono<Void> put(String viewerId, List<String> userIds) {
        return redis.opsForValue().set(PREFIX + viewerId, GsonUtils.getGson().toJson(userIds), TTL)
                .onErrorReturn(false)
                .then();
    }
}
