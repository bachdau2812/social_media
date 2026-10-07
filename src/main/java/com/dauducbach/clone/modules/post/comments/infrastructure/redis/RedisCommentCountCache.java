package com.dauducbach.clone.modules.post.comments.infrastructure.redis;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.comments.application.CommentCountCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Supplier;

@Slf4j
@Repository
@RequiredArgsConstructor
public class RedisCommentCountCache implements CommentCountCache {
    private static final String COUNT_PREFIX = "post_comment_count:";
    private static final String LOCK_PREFIX = "post_comment_count_lock:";
    private static final Duration COUNT_TTL = Duration.ofHours(24);
    private static final Duration LOCK_TTL = Duration.ofSeconds(5);
    private static final Duration RETRY_DELAY = Duration.ofMillis(50);
    private static final int RETRY_ATTEMPTS = 5;

    private final ReactiveRedisTemplate<String, String> redis;

    @Override
    public Mono<Long> getPostCount(String postId, Supplier<Mono<Long>> loadFromStore) {
        String key = countKey(postId);
        return read(key).switchIfEmpty(Mono.defer(() -> withLock(lockKey(postId),
                () -> read(key).switchIfEmpty(Mono.defer(() -> loadAndCache(key, loadFromStore))),
                String.format("Load post comment count cache failed for postId=%s", postId))));
    }

    @Override
    public Mono<Void> invalidatePostCount(String postId) {
        return withLock(lockKey(postId), () -> redis.delete(countKey(postId)).then(),
                        "Invalidate committed comment count for postId=" + postId)
                .onErrorResume(error -> {
                    log.warn("Committed comment; count cache invalidation failed for {}", postId, error);
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> adjustPostCount(String postId, long delta) {
        String key = countKey(postId);
        return withLock(lockKey(postId), () -> redis.opsForValue().increment(key, delta)
                        .flatMap(value -> value < 0
                                ? redis.opsForValue().set(key, "0", COUNT_TTL).then()
                                : redis.expire(key, COUNT_TTL).then()),
                String.format("Update post comment count cache failed for postId=%s", postId));
    }

    private Mono<Long> read(String key) {
        return redis.opsForValue().get(key)
                .filter(value -> value != null && !value.isBlank())
                .map(Long::parseLong);
    }

    private Mono<Long> loadAndCache(String key, Supplier<Mono<Long>> loadFromStore) {
        return loadFromStore.get()
                .flatMap(count -> redis.opsForValue().set(key, String.valueOf(count), COUNT_TTL).thenReturn(count));
    }

    private <T> Mono<T> withLock(String key, Supplier<Mono<T>> operation, String detailMessage) {
        return withLock(key, java.util.UUID.randomUUID().toString(), operation, RETRY_ATTEMPTS)
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.COMMENT_FETCH_FAILED, detailMessage, error));
    }

    private <T> Mono<T> withLock(String key, String token, Supplier<Mono<T>> operation, int attemptsLeft) {
        return redis.opsForValue().setIfAbsent(key, token, LOCK_TTL)
                .flatMap(acquired -> {
                    if (Boolean.TRUE.equals(acquired)) {
                        return operation.get()
                                .materialize()
                                .flatMap(signal -> releaseLock(key, token).thenReturn(signal))
                                .dematerialize();
                    }
                    if (attemptsLeft <= 0) {
                        return Mono.error(new AppException(ErrorCode.COMMENT_FETCH_FAILED, "Cannot acquire count cache lock"));
                    }
                    return Mono.delay(RETRY_DELAY).then(withLock(key, token, operation, attemptsLeft - 1));
                });
    }

    private Mono<Void> releaseLock(String key, String token) {
        return redis.opsForValue().get(key)
                .flatMap(currentToken -> token.equals(currentToken) ? redis.delete(key).then() : Mono.empty())
                .then();
    }

    private String countKey(String postId) {
        return COUNT_PREFIX + postId;
    }

    private String lockKey(String postId) {
        return LOCK_PREFIX + postId;
    }
}
