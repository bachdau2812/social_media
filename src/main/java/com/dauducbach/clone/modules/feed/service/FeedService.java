package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.feed.constant.FeedCacheKeys;
import com.dauducbach.clone.modules.feed.dto.cache.FeedPostDetailsCache;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedMediaResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.post.constant.OwnerType;
import com.dauducbach.clone.modules.post.entity.Media;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.service.CommentService;
import com.dauducbach.clone.modules.post.service.LikeService;
import com.dauducbach.clone.modules.post.service.MediaService;
import com.dauducbach.clone.modules.post.service.PostFeedQueryService;
import com.dauducbach.clone.modules.post.service.PostService;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import com.dauducbach.clone.utils.RedisUtil;
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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class FeedService {
    private static final Logger log = LoggerFactory.getLogger(FeedService.class);
    private static final String APPROVED_STATUS = "APPROVED";
    private static final int DEFAULT_LIMIT = 20;
    private static final int MAX_LIMIT = 50;
    private static final int REFILL_THRESHOLD = 10;
    private static final int REFILL_BATCH_SIZE = 80;
    private static final int SEEN_POST_LIMIT = 1000;
    private static final Duration FEED_TTL = Duration.ofDays(5);
    private static final Duration POST_DETAILS_TTL = Duration.ofDays(1);
    private static final Duration SEEN_POST_TTL = Duration.ofDays(5);

    ReactiveRedisTemplate<String, String> redisTemplate;
    PostService postService;
    PostFeedQueryService postFeedQueryService;
    MediaService mediaService;
    UserDetailsService userDetailsService;
    LikeService likeService;
    CommentService commentService;
    FeedVectorService feedVectorService;

    public Mono<FeedResponse> getFeed(String userId, int limit) {
        String cleanUserId = validateUserId(userId);
        int safeLimit = normalizeLimit(limit);

        log.info("|FeedService|getFeed|userId={}|limit={}", cleanUserId, safeLimit);
        return loadSeenPostIds(cleanUserId)
                .flatMap(seenPostIds -> loadFeedPostIds(cleanUserId, safeLimit, seenPostIds)
                        .flatMap(postIds -> {
                            if (postIds.size() >= safeLimit) {
                                return Mono.just(postIds);
                            }
                            return refillFeed(cleanUserId, safeLimit, seenPostIds, postIds)
                                    .then(loadFeedPostIds(cleanUserId, safeLimit, seenPostIds));
                        }))
                .flatMap(postIds -> hydrateFeedItems(cleanUserId, postIds)
                        .flatMap(items -> {
                            List<String> returnedIds = items.stream().map(FeedItemResponse::postId).toList();
                            return removeReturnedFeedIds(cleanUserId, postIds)
                                    .then(markSeenPosts(cleanUserId, returnedIds))
                                    .then(refillIfLow(cleanUserId, cleanUserId))
                                    .then(resolveHasMore(cleanUserId))
                                    .map(hasMore -> new FeedResponse(cleanUserId, safeLimit, items, hasMore));
                        }))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.POST_LIST_FETCH_FAILED,
                                String.format("Fetch feed failed for userId=%s", cleanUserId),
                                error));
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

        return loadCandidatePostIds(userId, candidateLimit, excludedPostIds)
                .flatMapMany(Flux::fromIterable)
                .concatMap(postId -> appendPostToUserFeed(userId, postId, Instant.now()))
                .then()
                .doOnSuccess(unused -> log.info("|FeedService|refillFeed|userId={}|excluded={}", userId, excludedPostIds.size()));
    }

    private Mono<List<String>> loadCandidatePostIds(String userId, int limit, Set<String> excludedPostIds) {
        Mono<List<String>> vectorCandidates = feedVectorService.buildQueryVector(userId)
                .flatMap(vector -> postFeedQueryService.searchRecommendedPostIds(vector, limit, excludedPostIds))
                .onErrorResume(error -> {
                    log.warn("|FeedService|loadCandidatePostIds|vector fallback|userId={}|error={}", userId, error.getMessage());
                    return Mono.just(List.of());
                });

        Mono<List<String>> recentCandidates = postFeedQueryService.getRecentApprovedPosts(limit, excludedPostIds)
                .map(PostDetails::getPostId)
                .collectList()
                .onErrorResume(error -> {
                    log.warn("|FeedService|loadCandidatePostIds|recent fallback|userId={}|error={}", userId, error.getMessage());
                    return Mono.just(List.of());
                });

        return Mono.zip(vectorCandidates, recentCandidates)
                .map(tuple -> mergeCandidates(tuple.getT1(), tuple.getT2(), limit));
    }

    private Mono<List<FeedItemResponse>> hydrateFeedItems(String userId, List<String> postIds) {
        return Flux.fromIterable(postIds)
                .concatMap(postId -> getFeedItem(userId, postId)
                        .onErrorResume(error -> {
                            log.warn("|FeedService|hydrateFeedItems|skip post|userId={}|postId={}|error={}",
                                    userId, postId, error.getMessage());
                            return Mono.empty();
                        }))
                .collectList();
    }

    private Mono<FeedItemResponse> getFeedItem(String userId, String postId) {
        return getCachedPostDetails(postId)
                .switchIfEmpty(Mono.defer(() -> buildAndCachePostDetails(postId)))
                .filter(cache -> APPROVED_STATUS.equalsIgnoreCase(cache.getValidateStatus()))
                .flatMap(cache -> likeService.hasLiked(userId, postId, EntityType.POST.name())
                        .onErrorReturn(false)
                        .map(liked -> toResponse(cache, liked)));
    }

    private Mono<FeedPostDetailsCache> getCachedPostDetails(String postId) {
        return redisTemplate.opsForValue()
                .get(FeedCacheKeys.postDetails(postId))
                .map(json -> RedisUtil.deserialize(json, FeedPostDetailsCache.class))
                .filter(cache -> cache != null && cache.getPostId() != null && !cache.getPostId().isBlank())
                .onErrorResume(error -> {
                    log.warn("|FeedService|getCachedPostDetails|failed|postId={}|error={}", postId, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<FeedPostDetailsCache> buildAndCachePostDetails(String postId) {
        return postService.getPostById(postId)
                .filter(post -> APPROVED_STATUS.equalsIgnoreCase(post.getValidateStatus()))
                .flatMap(this::buildPostDetailsCache)
                .flatMap(cache -> {
                    String json = RedisUtil.serialize(cache);
                    if (json == null) {
                        return Mono.just(cache);
                    }
                    return redisTemplate.opsForValue()
                            .set(FeedCacheKeys.postDetails(postId), json, POST_DETAILS_TTL)
                            .thenReturn(cache);
                });
    }

    private Mono<FeedPostDetailsCache> buildPostDetailsCache(PostDetails post) {
        Mono<String> authorUsername = userDetailsService.getUserDetailsById(post.getUserId())
                .map(UserDetails::getUsername)
                .filter(username -> username != null && !username.isBlank())
                .defaultIfEmpty(post.getUserId())
                .onErrorReturn(post.getUserId());

        Mono<List<FeedMediaResponse>> media = mediaService.getByOwnerId(post.getPostId(), OwnerType.POST)
                .map(this::toMediaResponse)
                .collectList()
                .onErrorReturn(List.of());

        Mono<Long> likeCount = likeService.countLikes(post.getPostId(), EntityType.POST.name()).onErrorReturn(0L);
        Mono<Long> commentCount = commentService.countCommentsByPostId(post.getPostId()).onErrorReturn(0L);

        return Mono.zip(authorUsername, media, likeCount, commentCount)
                .map(tuple -> FeedPostDetailsCache.builder()
                        .postId(post.getPostId())
                        .userId(post.getUserId())
                        .content(post.getContent())
                        .hashtag(post.getHashtag())
                        .hashtags(post.getHashtagList())
                        .createdAt(post.getCreatedAt())
                        .updatedAt(post.getUpdatedAt())
                        .validateStatus(post.getValidateStatus())
                        .authorUsername(tuple.getT1())
                        .media(tuple.getT2())
                        .likeCount(tuple.getT3())
                        .commentCount(tuple.getT4())
                        .build());
    }

    private FeedItemResponse toResponse(FeedPostDetailsCache cache, boolean likedByCurrentUser) {
        return new FeedItemResponse(
                cache.getPostId(),
                cache.getUserId(),
                cache.getAuthorUsername(),
                cache.getContent(),
                cache.getHashtags() == null ? List.of() : cache.getHashtags(),
                cache.getMedia() == null ? List.of() : cache.getMedia(),
                cache.getLikeCount(),
                cache.getCommentCount(),
                likedByCurrentUser,
                cache.getCreatedAt(),
                cache.getUpdatedAt()
        );
    }

    private FeedMediaResponse toMediaResponse(Media media) {
        return new FeedMediaResponse(
                media.getAssetId(),
                media.getPublicId(),
                media.getMediaFormat(),
                media.getResourceType(),
                media.getUrl(),
                media.getSecureUrl(),
                media.getDisplayName()
        );
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

    private List<String> mergeCandidates(List<String> first, List<String> second, int limit) {
        LinkedHashSet<String> merged = new LinkedHashSet<>();
        if (first != null) {
            merged.addAll(first);
        }
        if (second != null) {
            merged.addAll(second);
        }
        return new ArrayList<>(merged).stream().limit(limit).toList();
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
