package com.dauducbach.clone.modules.feed.infrastructure.redis;

import com.dauducbach.clone.modules.feed.constant.FeedCacheKeys;
import com.dauducbach.clone.modules.feed.service.FeedSeenPostStore;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Repository
@RequiredArgsConstructor
public class RedisFeedSeenPostStore implements FeedSeenPostStore {
    private static final Logger log = LoggerFactory.getLogger(RedisFeedSeenPostStore.class);
    private static final int SEEN_POST_LIMIT = 1000;
    private static final Duration SEEN_POST_TTL = Duration.ofDays(5);

    private final ReactiveRedisTemplate<String, String> redis;

    @Override
    public Mono<Set<String>> load(String viewerId) {
        return redis.opsForList().range(FeedCacheKeys.seenPost(viewerId), 0, -1)
                .filter(postId -> postId != null && !postId.isBlank())
                .collectList()
                .map(ids -> (Set<String>) new LinkedHashSet<>(ids))
                .onErrorResume(error -> {
                    log.warn("|RedisFeedSeenPostStore|load|failed|viewerId={}|error={}", viewerId, error.getMessage());
                    return Mono.just(new LinkedHashSet<>());
                });
    }

    @Override
    public Mono<Void> mark(String viewerId, List<String> postIds) {
        if (postIds == null || postIds.isEmpty()) return Mono.empty();
        String key = FeedCacheKeys.seenPost(viewerId);
        return Flux.fromIterable(postIds)
                .concatMap(postId -> redis.opsForList().rightPush(key, postId))
                .then(redis.opsForList().trim(key, -SEEN_POST_LIMIT, -1))
                .then(redis.expire(key, SEEN_POST_TTL))
                .then()
                .onErrorResume(error -> {
                    log.warn("|RedisFeedSeenPostStore|mark|failed|viewerId={}|count={}|error={}",
                            viewerId, postIds.size(), error.getMessage());
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> clear(String viewerId) {
        return redis.delete(FeedCacheKeys.seenPost(viewerId)).then()
                .onErrorResume(error -> {
                    log.warn("|RedisFeedSeenPostStore|clear|failed|viewerId={}|error={}", viewerId, error.getMessage());
                    return Mono.empty();
                });
    }
}
