package com.dauducbach.clone.modules.post.infrastructure.redis;

import com.dauducbach.clone.modules.post.application.PostDetailsCache;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.infrastructure.redis.RedisJsonCodec;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
public class RedisPostDetailsCache implements PostDetailsCache {
    private static final Logger log = LoggerFactory.getLogger(RedisPostDetailsCache.class);
    private static final String KEY_PREFIX = "post:details:v3:";
    private static final Duration TTL = Duration.ofHours(24);

    private final ReactiveRedisTemplate<String, String> redis;

    @Override
    public Mono<PostDetails> get(String postId) {
        return redis.opsForValue().get(key(postId))
                .onErrorResume(error -> {
                    log.warn("|RedisPostDetailsCache|get|cache read failed|postId={}|error={}", postId, error.getMessage());
                    return Mono.empty();
                })
                .flatMap(serialized -> {
                    PostDetails cached = RedisJsonCodec.deserialize(serialized, PostDetails.class);
                    if (cached == null) {
                        log.warn("|RedisPostDetailsCache|get|cache value could not be deserialized|postId={}", postId);
                        return Mono.empty();
                    }
                    return Mono.just(cached);
                });
    }

    @Override
    public Mono<Boolean> put(PostDetails post) {
        String serialized = RedisJsonCodec.serialize(post);
        return serialized == null ? Mono.just(false) : redis.opsForValue().set(key(post.getPostId()), serialized, TTL);
    }

    @Override
    public Mono<Void> evict(String postId) {
        return redis.opsForValue().delete(key(postId)).then();
    }

    @Override
    public Mono<Void> evictAll(List<String> postIds) {
        return Flux.fromIterable(postIds).flatMap(this::evict).then();
    }

    private static String key(String postId) {
        return KEY_PREFIX + postId;
    }
}
