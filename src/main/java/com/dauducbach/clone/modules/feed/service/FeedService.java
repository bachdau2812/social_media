package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.feed.dto.FeedVectorSnapshot;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.publicapi.PostFeedQuery;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

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

    FeedSeenPostStore seenPosts;
    PostFeedQuery postFeedQuery;
    FeedCandidatePipeline candidatePipeline;
    FeedItemHydrator itemHydrator;
    FeedVectorSnapshotService vectorSnapshots;
    FeedQueue queue;
    MixedFeedService mixedFeedService;
    com.dauducbach.clone.configuration.PostPopularityProperties popularityProperties;

    public Mono<FeedResponse> getFeed(String userId, int limit) {
        return getFeed(userId, limit, MediaDisplayType.FEED);
    }

    public Mono<FeedResponse> getFeed(String userId, int limit, MediaDisplayType mediaType) {
        return getFeed(userId, limit, mediaType, null);
    }

    public Mono<FeedResponse> getFeed(String userId, int limit, MediaDisplayType mediaType, String cursor) {
        String cleanUserId = validateUserId(userId);
        int safeLimit = normalizeLimit(limit);
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.FEED : mediaType;

        if (popularityProperties != null && popularityProperties.isMixedFeedEnabled()) {
            return mixedFeedService.getFeed(cleanUserId, safeLimit, displayType, cursor);
        }
        if (cursor != null) return Mono.error(new AppException(ErrorCode.FEED_CURSOR_INVALID));

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

        return postFeedQuery
                .findRecentFriendFeedActivities(cleanUserId, safeLimit + 1, offset)
                .collectList()
                .flatMap(activities -> {
                    boolean hasMore = activities.size() > safeLimit;
                    return itemHydrator.hydrateFriendActivities(
                                    cleanUserId, activities.stream().limit(safeLimit).toList(), displayType)
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
        return queue.readForServe(userId, limit, seenPostIds)
                .flatMap(read -> {
                    if (read.legacy() || (!read.needsRefill() && read.postIds().size() >= limit)) {
                        return Mono.just(read.postIds());
                    }
                    return refillFeed(userId, limit, seenPostIds, read.postIds())
                            .flatMap(committed -> queue.readForServe(userId, limit, seenPostIds)
                                    .flatMap(fresh -> committed || fresh.legacy() || fresh.postIds().size() >= limit
                                            ? Mono.just(fresh.postIds())
                                            : recentFallback(limit, seenPostIds, fresh.postIds())));
                });
    }

    private Mono<List<String>> recentFallback(int limit, Set<String> seen, List<String> cached) {
        Set<String> excluded = new LinkedHashSet<>(seen);
        excluded.addAll(cached);
        return postFeedQuery.findRecentApprovedPostIds(limit - cached.size(), excluded)
                .collectList().map(recent -> {
                    List<String> ids = new java.util.ArrayList<>(cached);
                    ids.addAll(recent);
                    return List.copyOf(ids);
                });
    }

    public Mono<Void> appendPostToUserFeed(String userId, String postId, Instant eventTime) {
        if (userId == null || userId.isBlank() || postId == null || postId.isBlank()) return Mono.empty();
        return queue.appendFanout(userId, postId, eventTime)
                .doOnSuccess(unused -> log.info("|FeedService|appendPostToUserFeed|userId={}|postId={}", userId, postId))
                .doOnError(error -> log.warn("|FeedService|appendPostToUserFeed|failed|userId={}|postId={}|error={}",
                        userId, postId, error.getMessage()));
    }

    private Mono<Boolean> refillFeed(String userId, int limit, Set<String> seenPostIds, List<String> currentPostIds) {
        Set<String> excludedPostIds = new LinkedHashSet<>(seenPostIds);
        excludedPostIds.addAll(currentPostIds);
        int candidateLimit = Math.max(REFILL_BATCH_SIZE, limit * 3);
        return refillBatch(userId, candidateLimit, excludedPostIds, 0);
    }

    private Mono<Boolean> refillBatch(String userId, int limit, Set<String> excluded, int staleRetries) {
        return vectorSnapshots.load(userId).defaultIfEmpty(new FeedVectorSnapshot(0L, List.of()))
                .flatMap(snapshot -> candidatePipeline.select(snapshot, limit, excluded)
                        .flatMap(candidates -> queue.commit(userId, snapshot.version(), candidates)))
                .flatMap(committed -> committed || staleRetries >= 2 ? Mono.just(committed)
                        : Mono.defer(() -> refillBatch(userId, limit, excluded, staleRetries + 1)));
    }

    private Mono<List<FeedItemResponse>> hydrateFeedItems(
            String userId,
            List<String> postIds,
            MediaDisplayType mediaType
    ) {
        return itemHydrator.hydratePage(userId, postIds, mediaType);
    }

    public Mono<FeedItemResponse> getFeedItemForViewer(
            String userId,
            String postId,
            MediaDisplayType mediaType
    ) {
        return itemHydrator.hydrate(userId, postId, mediaType);
    }
    private Mono<Set<String>> loadSeenPostIds(String userId) {
        return seenPosts.load(userId);
    }

    private Mono<Void> clearSeenPosts(String userId) {
        return seenPosts.clear(userId)
                .doOnSuccess(unused -> log.info("|FeedService|clearSeenPosts|userId={}", userId));
    }

    private Mono<Void> markSeenPosts(String userId, List<String> postIds) {
        return seenPosts.mark(userId, postIds);
    }

    private Mono<Void> removeReturnedFeedIds(String userId, List<String> postIds) {
        return queue.removeReturned(userId, postIds);
    }

    private Mono<Void> refillIfLow(String userId, String currentUserId) {
        return queue.readForServe(userId, REFILL_BATCH_SIZE, Set.of())
                .flatMap(read -> !read.legacy() && (read.needsRefill() || read.postIds().size() < REFILL_THRESHOLD)
                        ? loadSeenPostIds(currentUserId).flatMap(seen -> refillFeed(userId, DEFAULT_LIMIT, seen, read.postIds())).then()
                        : Mono.empty());
    }

    private Mono<Boolean> resolveHasMore(String userId) {
        return queue.hasMore(userId);
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
