package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.feed.constant.FeedCacheKeys;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.service.PostFeedQueryService;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class FeedService {
    private static final Logger log = LoggerFactory.getLogger(FeedService.class);
    private static final int DEFAULT_LIMIT = 20;
    private static final int MAX_LIMIT = 50;
    private static final int REFILL_THRESHOLD = 10;
    private static final int REFILL_BATCH_SIZE = 80;
    private static final int SEEN_POST_LIMIT = 1000;
    private static final Duration FEED_TTL = Duration.ofDays(5);
    private static final Duration SEEN_POST_TTL = Duration.ofDays(5);

    ReactiveRedisTemplate<String, String> redisTemplate;
    PostFeedQueryService postFeedQueryService;
    FeedCandidatePipeline candidatePipeline;
    FeedItemHydrator itemHydrator;

    public Mono<FeedResponse> getFeed(String userId, int limit) {
        return getFeed(userId, limit, MediaDisplayType.FEED);
    }

    public Mono<FeedResponse> getFeed(String userId, int limit, MediaDisplayType mediaType) {
        String cleanUserId = validateUserId(userId);
        int safeLimit = normalizeLimit(limit);
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.FEED : mediaType;

        log.info("|FeedService|getFeed|userId={}|limit={}|mediaType={}", cleanUserId, safeLimit, displayType);
        return buildFeedResponse(cleanUserId, safeLimit, false, displayType)
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.POST_LIST_FETCH_FAILED,
                                String.format("Fetch feed failed for userId=%s", cleanUserId),
                                error));
    }

    public Mono<FeedResponse> getFriendsFeed(
            String userId,
            int limit,
            int page,
            MediaDisplayType mediaType
    ) {
        String cleanUserId = validateUserId(userId);
        int safeLimit = normalizeLimit(limit);
        int safePage = Math.max(page, 0);
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.FEED : mediaType;
        int offset = safePage * safeLimit;

        return postFeedQueryService
                .getRecentApprovedPostsFromMutualFriends(cleanUserId, safeLimit + 1, offset)
                .collectList()
                .flatMap(posts -> {
                    boolean hasMore = posts.size() > safeLimit;
                    return Flux.fromIterable(posts.stream().limit(safeLimit).toList())
                            .concatMap(post -> getFeedItemForViewer(cleanUserId, post.getPostId(), displayType))
                            .collectList()
                            .map(items -> new FeedResponse(cleanUserId, safeLimit, items, hasMore));
                });
    }
    private Mono<FeedResponse> buildFeedResponse(
            String userId,
            int limit,
            boolean retriedAfterSeenReset,
            MediaDisplayType mediaType
    ) {
        return loadSeenPostIds(userId)
                .flatMap(seenPostIds -> loadPostIdsForRequest(userId, limit, seenPostIds)
                        .flatMap(postIds -> hydrateFeedItems(userId, postIds, mediaType)
                                .flatMap(items -> {
                                    if (items.isEmpty() && !retriedAfterSeenReset && !seenPostIds.isEmpty()) {
                                        return removeReturnedFeedIds(userId, postIds)
                                                .then(clearSeenPosts(userId))
                                                .then(buildFeedResponse(userId, limit, true, mediaType));
                                    }

                                    List<String> returnedIds = items.stream().map(FeedItemResponse::postId).toList();
                                    return removeReturnedFeedIds(userId, postIds)
                                            .then(markSeenPosts(userId, returnedIds))
                                            .then(refillIfLow(userId, userId))
                                            .then(resolveHasMore(userId))
                                            .map(hasMore -> new FeedResponse(userId, limit, items, hasMore));
                                })));
    }
    private Mono<List<String>> loadPostIdsForRequest(String userId, int limit, Set<String> seenPostIds) {
        return loadFeedPostIds(userId, limit, seenPostIds)
                .flatMap(postIds -> {
                    if (postIds.size() >= limit) {
                        return Mono.just(postIds);
                    }
                    return refillFeed(userId, limit, seenPostIds, postIds)
                            .then(loadFeedPostIds(userId, limit, seenPostIds));
                });
    }

    public Mono<Void> appendPostToUserFeed(String userId, String postId, Instant eventTime) {
        if (userId == null || userId.isBlank() || postId == null || postId.isBlank()) {
            return Mono.empty();
        }

        String feedKey = FeedCacheKeys.userFeed(userId);
        double score = eventTime == null ? Instant.now().toEpochMilli() : eventTime.toEpochMilli();
        return redisTemplate.opsForZSet()
                .add(feedKey, postId, score)
                .then(redisTemplate.expire(feedKey, FEED_TTL))
                .then()
                .doOnSuccess(unused -> log.info("|FeedService|appendPostToUserFeed|userId={}|postId={}", userId, postId))
                .onErrorResume(error -> {
                    log.warn("|FeedService|appendPostToUserFeed|failed|userId={}|postId={}|error={}",
                            userId, postId, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> appendCandidateToUserFeed(String userId, FeedCandidate candidate) {
        if (candidate == null || candidate.postId() == null || candidate.postId().isBlank()) {
            return Mono.empty();
        }
        String feedKey = FeedCacheKeys.userFeed(userId);
        return redisTemplate.opsForZSet()
                .add(feedKey, candidate.postId(), candidate.deliveryScore())
                .then(redisTemplate.expire(feedKey, FEED_TTL))
                .then()
                .doOnSuccess(unused -> log.info(
                        "|FeedService|appendCandidateToUserFeed|userId={}|postId={}|source={}|score={}",
                        userId, candidate.postId(), candidate.sourceType(), candidate.deliveryScore()))
                .onErrorResume(error -> {
                    log.warn("|FeedService|appendCandidateToUserFeed|failed|userId={}|postId={}|error={}",
                            userId, candidate.postId(), error.getMessage());
                    return Mono.empty();
                });
    }
    private Mono<List<String>> loadFeedPostIds(String userId, int limit, Set<String> seenPostIds) {
        return redisTemplate.opsForZSet()
                .reverseRange(FeedCacheKeys.userFeed(userId), Range.closed(0L, (long) REFILL_BATCH_SIZE - 1))
                .filter(postId -> postId != null && !postId.isBlank())
                .filter(postId -> !seenPostIds.contains(postId))
                .distinct()
                .take(limit)
                .collectList()
                .onErrorResume(error -> {
                    log.warn("|FeedService|loadFeedPostIds|cache read failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.just(List.of());
                });
    }

    private Mono<Void> refillFeed(String userId, int limit, Set<String> seenPostIds, List<String> currentPostIds) {
        Set<String> excludedPostIds = new LinkedHashSet<>(seenPostIds);
        excludedPostIds.addAll(currentPostIds);
        int candidateLimit = Math.max(REFILL_BATCH_SIZE, limit * 3);

        return candidatePipeline.select(userId, candidateLimit, excludedPostIds)
                .flatMapMany(Flux::fromIterable)
                .concatMap(candidate -> appendCandidateToUserFeed(userId, candidate))
                .then()
                .doOnSuccess(unused -> log.info("|FeedService|refillFeed|userId={}|excluded={}", userId, excludedPostIds.size()));
    }


    private Mono<List<FeedItemResponse>> hydrateFeedItems(
            String userId,
            List<String> postIds,
            MediaDisplayType mediaType
    ) {
        return Flux.fromIterable(postIds)
                .concatMap(postId -> getFeedItemForViewer(userId, postId, mediaType)
                        .onErrorResume(error -> {
                            log.warn("|FeedService|hydrateFeedItems|skip post|userId={}|postId={}|error={}",
                                    userId, postId, error.getMessage());
                            return Mono.empty();
                        }))
                .collectList();
    }

    public Mono<FeedItemResponse> getFeedItemForViewer(
            String userId,
            String postId,
            MediaDisplayType mediaType
    ) {
        return itemHydrator.hydrate(userId, postId, mediaType);
    }
    private Mono<Set<String>> loadSeenPostIds(String userId) {
        return redisTemplate.opsForList()
                .range(FeedCacheKeys.seenPost(userId), 0, -1)
                .filter(postId -> postId != null && !postId.isBlank())
                .collectList()
                .map(postIds -> (Set<String>) new LinkedHashSet<>(postIds))
                .onErrorResume(error -> {
                    log.warn("|FeedService|loadSeenPostIds|failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.just(new LinkedHashSet<>());
                });
    }

    private Mono<Void> clearSeenPosts(String userId) {
        return redisTemplate.delete(FeedCacheKeys.seenPost(userId))
                .then()
                .doOnSuccess(unused -> log.info("|FeedService|clearSeenPosts|userId={}", userId))
                .onErrorResume(error -> {
                    log.warn("|FeedService|clearSeenPosts|failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> markSeenPosts(String userId, List<String> postIds) {
        if (postIds == null || postIds.isEmpty()) {
            return Mono.empty();
        }

        String seenKey = FeedCacheKeys.seenPost(userId);
        return Flux.fromIterable(postIds)
                .concatMap(postId -> redisTemplate.opsForList().rightPush(seenKey, postId))
                .then(redisTemplate.opsForList().trim(seenKey, -SEEN_POST_LIMIT, -1))
                .then(redisTemplate.expire(seenKey, SEEN_POST_TTL))
                .then()
                .onErrorResume(error -> {
                    log.warn("|FeedService|markSeenPosts|failed|userId={}|count={}|error={}",
                            userId, postIds.size(), error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> removeReturnedFeedIds(String userId, List<String> postIds) {
        if (postIds == null || postIds.isEmpty()) {
            return Mono.empty();
        }

        return redisTemplate.opsForZSet()
                .remove(FeedCacheKeys.userFeed(userId), postIds.toArray())
                .then()
                .onErrorResume(error -> {
                    log.warn("|FeedService|removeReturnedFeedIds|failed|userId={}|count={}|error={}",
                            userId, postIds.size(), error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> refillIfLow(String userId, String currentUserId) {
        return redisTemplate.opsForZSet()
                .size(FeedCacheKeys.userFeed(userId))
                .defaultIfEmpty(0L)
                .flatMap(size -> size < REFILL_THRESHOLD
                        ? loadSeenPostIds(currentUserId).flatMap(seen -> refillFeed(userId, DEFAULT_LIMIT, seen, List.of()))
                        : Mono.empty())
                .onErrorResume(error -> Mono.empty());
    }

    private Mono<Boolean> resolveHasMore(String userId) {
        return redisTemplate.opsForZSet()
                .size(FeedCacheKeys.userFeed(userId))
                .map(size -> size != null && size > 0)
                .defaultIfEmpty(false)
                .onErrorReturn(false);
    }


    private String validateUserId(String userId) {
        if (userId == null || userId.isBlank()) {
            throw new AppException(ErrorCode.POST_LIST_FETCH_FAILED, "userId is required");
        }
        return userId.trim();
    }

    private int normalizeLimit(int limit) {
        if (limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }
}
