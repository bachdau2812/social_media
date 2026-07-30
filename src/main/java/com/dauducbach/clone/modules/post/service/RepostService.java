package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.response.RepostToggleResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostRepost;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repositoty.PostRepostRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class RepostService {
    private static final Logger log = LoggerFactory.getLogger(RepostService.class);
    private static final String POST_REPOST_COUNT_PREFIX = "post_repost_count:";
    private static final String POST_REPOST_COUNT_LOCK_PREFIX = "post_repost_count_lock:";
    private static final Duration COUNT_CACHE_TTL = Duration.ofHours(24);
    private static final Duration COUNT_LOCK_TTL = Duration.ofSeconds(5);
    private static final Duration COUNT_LOCK_RETRY_DELAY = Duration.ofMillis(50);
    private static final int COUNT_LOCK_RETRY_ATTEMPTS = 5;
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 100;

    PostRepostRepository repostRepository;
    PostDetailsRepository postDetailsRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;

    public Mono<RepostToggleResponse> repost(String actorId, String postId) {
        validateRequiredIds(actorId, postId);
        return postDetailsRepository.findById(postId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.POST_NOT_FOUND, "Post not found for repost")))
                .flatMap(post -> {
                    if (actorId.equals(post.getUserId())) {
                        return Mono.error(new AppException(ErrorCode.REPOST_OWN_POST_NOT_ALLOWED, "Cannot repost your own post"));
                    }
                    return repostRepository.findByActorIdAndPostId(actorId, postId)
                            .flatMap(existing -> countReposts(postId)
                                    .map(count -> new RepostToggleResponse(postId, true, existing.getId(), count)))
                            .switchIfEmpty(Mono.defer(() -> createRepost(actorId, post)));
                })
                .doOnSuccess(response -> log.info("|RepostService|repost|actorId={}|postId={}|reposted={}|count={}", actorId, postId, response.reposted(), response.repostCount()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.REPOST_CREATE_FAILED, "Create repost failed", error));
    }

    public Mono<RepostToggleResponse> unrepost(String actorId, String postId) {
        validateRequiredIds(actorId, postId);
        return repostRepository.findByActorIdAndPostId(actorId, postId)
                .flatMap(existing -> ensurePostRepostCountCache(postId)
                        .then(repostRepository.delete(existing))
                        .then(updatePostRepostCountCache(postId, -1))
                        .then(countReposts(postId))
                        .map(count -> new RepostToggleResponse(postId, false, existing.getId(), count)))
                .switchIfEmpty(countReposts(postId).map(count -> new RepostToggleResponse(postId, false, null, count)))
                .doOnSuccess(response -> log.info("|RepostService|unrepost|actorId={}|postId={}|count={}", actorId, postId, response.repostCount()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.REPOST_DELETE_FAILED, "Delete repost failed", error));
    }

    public Mono<Boolean> hasReposted(String actorId, String postId) {
        validateRequiredIds(actorId, postId);
        return repostRepository.existsByActorIdAndPostId(actorId, postId)
                .onErrorMap(error -> new AppException(ErrorCode.REPOST_FETCH_FAILED, "Check repost status failed", error));
    }

    public Mono<Long> countReposts(String postId) {
        validatePostId(postId);
        return getPostRepostCountFromCache(postId)
                .doOnSuccess(count -> log.info("|RepostService|countReposts|postId={}|count={}", postId, count))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.REPOST_FETCH_FAILED, "Count reposts failed", error));
    }

    public Mono<PageResponse<String>> getRepostedPostIds(String actorId, int page, int size) {
        validateActorId(actorId);
        int pageNumber = Math.max(page, 0);
        int pageSize = validatePageSize(size);
        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        return repostRepository.countByActorId(actorId)
                .flatMap(total -> repostRepository.findPostIdsByActorId(actorId, pageable)
                        .collectList()
                        .map(content -> PageResponse.of(content, pageNumber, total, pageSize)))
                .onErrorMap(error -> new AppException(ErrorCode.REPOST_FETCH_FAILED, "Fetch reposted posts failed", error));
    }

    public Mono<PageResponse<String>> getReposterActorIds(String postId, int page, int size) {
        validatePostId(postId);
        int pageNumber = Math.max(page, 0);
        int pageSize = validatePageSize(size);
        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        return countReposts(postId)
                .flatMap(total -> repostRepository.findActorIdsByPostId(postId, pageable)
                        .collectList()
                        .map(content -> PageResponse.of(content, pageNumber, total, pageSize)))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.REPOST_FETCH_FAILED, "Fetch reposter actors failed", error));
    }
    public Flux<PostDetails> getRepostedPosts(String actorId, int limit) {
        validateActorId(actorId);
        int safeLimit = limit <= 0 ? DEFAULT_PAGE_SIZE : Math.min(limit, MAX_PAGE_SIZE);
        return repostRepository.findPostIdsByActorId(actorId, PageRequest.of(0, safeLimit))
                .concatMap(postDetailsRepository::findById)
                .filter(post -> post.getPostId() != null)
                .filter(post -> "APPROVED".equalsIgnoreCase(post.getValidateStatus()));
    }

    private Mono<RepostToggleResponse> createRepost(String actorId, PostDetails post) {
        PostRepost repost = PostRepost.builder()
                .id(UUID.randomUUID().toString())
                .actorId(actorId)
                .postId(post.getPostId())
                .postOwnerId(post.getUserId())
                .createdAt(Instant.now())
                .build();
        return ensurePostRepostCountCache(post.getPostId())
                .then(r2dbcEntityTemplate.insert(PostRepost.class).using(repost))
                .flatMap(saved -> updatePostRepostCountCache(post.getPostId(), 1)
                        .then(countReposts(post.getPostId()))
                        .map(count -> new RepostToggleResponse(post.getPostId(), true, saved.getId(), count)));
    }

    private Mono<Long> getPostRepostCountFromCache(String postId) {
        String cacheKey = postRepostCountKey(postId);
        return readLongCache(cacheKey)
                .doOnNext(count -> log.info("|RepostService|getPostRepostCountFromCache|cache hit|postId={}|count={}", postId, count))
                .switchIfEmpty(Mono.defer(() -> withCountLock(
                        POST_REPOST_COUNT_LOCK_PREFIX + postId,
                        () -> readLongCache(cacheKey)
                                .switchIfEmpty(loadAndSetCountCache(cacheKey, () -> repostRepository.countByPostId(postId))),
                        "Load repost count cache failed for postId=" + postId
                )));
    }

    private Mono<Void> ensurePostRepostCountCache(String postId) {
        String cacheKey = postRepostCountKey(postId);
        return withCountLock(
                POST_REPOST_COUNT_LOCK_PREFIX + postId,
                () -> readLongCache(cacheKey)
                        .switchIfEmpty(Mono.defer(() -> loadAndSetCountCache(cacheKey, () -> repostRepository.countByPostId(postId))))
                        .then(),
                "Ensure repost count cache failed for postId=" + postId
        );
    }

    private Mono<Void> updatePostRepostCountCache(String postId, long delta) {
        String cacheKey = postRepostCountKey(postId);
        return withCountLock(
                POST_REPOST_COUNT_LOCK_PREFIX + postId,
                () -> reactiveRedisStringTemplate.opsForValue().increment(cacheKey, delta)
                        .flatMap(value -> value < 0
                                ? reactiveRedisStringTemplate.opsForValue().set(cacheKey, "0", COUNT_CACHE_TTL).then()
                                : reactiveRedisStringTemplate.expire(cacheKey, COUNT_CACHE_TTL).then()),
                "Update repost count cache failed for postId=" + postId
        );
    }

    private Mono<Long> readLongCache(String cacheKey) {
        return reactiveRedisStringTemplate.opsForValue()
                .get(cacheKey)
                .filter(value -> value != null && !value.isBlank())
                .map(Long::parseLong);
    }

    private Mono<Long> loadAndSetCountCache(String cacheKey, Supplier<Mono<Long>> dbCountSupplier) {
        return dbCountSupplier.get()
                .defaultIfEmpty(0L)
                .flatMap(count -> reactiveRedisStringTemplate.opsForValue()
                        .set(cacheKey, String.valueOf(count), COUNT_CACHE_TTL)
                        .thenReturn(count));
    }

    private <T> Mono<T> withCountLock(String lockKey, Supplier<Mono<T>> operation, String detailMessage) {
        return withCountLock(lockKey, UUID.randomUUID().toString(), operation, COUNT_LOCK_RETRY_ATTEMPTS)
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.REPOST_FETCH_FAILED, detailMessage, error));
    }

    private <T> Mono<T> withCountLock(String lockKey, String lockToken, Supplier<Mono<T>> operation, int attemptsLeft) {
        return reactiveRedisStringTemplate.opsForValue()
                .setIfAbsent(lockKey, lockToken, COUNT_LOCK_TTL)
                .flatMap(acquired -> {
                    if (Boolean.TRUE.equals(acquired)) {
                        return operation.get()
                                .materialize()
                                .flatMap(signal -> releaseCountLock(lockKey, lockToken).thenReturn(signal))
                                .dematerialize();
                    }
                    if (attemptsLeft <= 0) {
                        return Mono.error(new AppException(ErrorCode.REPOST_FETCH_FAILED, "Cannot acquire repost count cache lock"));
                    }
                    return Mono.delay(COUNT_LOCK_RETRY_DELAY)
                            .then(withCountLock(lockKey, lockToken, operation, attemptsLeft - 1));
                });
    }

    private Mono<Void> releaseCountLock(String lockKey, String lockToken) {
        return reactiveRedisStringTemplate.opsForValue()
                .get(lockKey)
                .flatMap(currentToken -> lockToken.equals(currentToken)
                        ? reactiveRedisStringTemplate.delete(lockKey).then()
                        : Mono.empty())
                .then();
    }

    private String postRepostCountKey(String postId) {
        return POST_REPOST_COUNT_PREFIX + postId;
    }

    private void validateRequiredIds(String actorId, String postId) {
        validateActorId(actorId);
        validatePostId(postId);
    }

    private void validateActorId(String actorId) {
        if (actorId == null || actorId.isBlank()) {
            throw new AppException(ErrorCode.REPOST_CREATE_FAILED, "actorId is required");
        }
    }

    private void validatePostId(String postId) {
        if (postId == null || postId.isBlank()) {
            throw new AppException(ErrorCode.REPOST_CREATE_FAILED, "postId is required");
        }
    }

    private int validatePageSize(int size) {
        if (size <= 0) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(size, MAX_PAGE_SIZE);
    }
}
