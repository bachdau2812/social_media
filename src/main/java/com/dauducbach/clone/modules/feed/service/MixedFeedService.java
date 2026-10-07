package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.feed.dto.FeedCursorState;
import com.dauducbach.clone.modules.feed.dto.FeedSourceCursor;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** Each request owns its scan state; only consumed candidates advance signed source anchors. */
@Service
@RequiredArgsConstructor
public class MixedFeedService {
    private static final Logger log = LoggerFactory.getLogger(MixedFeedService.class);
    private final FriendFeedCandidateSource friends;
    private final PopularFeedCandidateSource popular;
    private final FeedItemHydrator hydrator;
    private final FeedSeenPostStore seenPosts;
    private final FeedCursorCodec codec;
    private final PostPopularityProperties properties;

    public Mono<FeedResponse> getFeed(String userId, int limit, MediaDisplayType mediaType, String cursor) {
        return Mono.defer(() -> {
            if (userId == null || userId.isBlank()) return Mono.error(new AppException(ErrorCode.FEED_REQUEST_INVALID));
            String viewer = userId.trim();
            int size = limit <= 0 ? 20 : Math.min(limit, 50);
            MediaDisplayType media = mediaType == null ? MediaDisplayType.FEED : mediaType;
            Instant now = Instant.now();
            FeedCursorState state = cursor == null ? codec.start(viewer, media, now) : codec.decode(cursor, viewer, media, now);
            Instant upper = Instant.ofEpochMilli(state.startedAt());
            Scan friendScan = new Scan(state.friends(), state.friendsExhausted());
            Scan popularScan = new Scan(state.popular(), state.popularExhausted());
            List<FeedItemResponse> selected = new ArrayList<>();
            Set<String> selectedIds = new HashSet<>();
            return loadSeen(viewer).flatMap(seen -> scan(viewer, media, friendScan, (size + 1) / 2,
                            after -> friends.find(viewer, upper, after, batchSize(friendScan)),
                            false, seen, selectedIds, selected)
                    .then(scan(viewer, media, popularScan, size / 2,
                            after -> popular.find(upper, now, after, batchSize(popularScan)),
                            true, seen, selectedIds, selected))
                    .then(Mono.defer(() -> {
                        if (popularScan.unavailable) {
                            // The partial popular selection was never returned: retry it from the input anchor.
                            selected.subList(friendScan.returned, selected.size()).clear();
                            popularScan.anchor = state.popular();
                            popularScan.exhausted = state.popularExhausted();
                        }
                        boolean hasMore = !friendScan.exhausted || (size / 2 > 0 && !popularScan.exhausted);
                        List<FeedItemResponse> ordered = interleave(selected, friendScan.returned);
                        FeedCursorState next = new FeedCursorState(viewer, media.name(), FeedCursorCodec.VERSION,
                                state.startedAt(), state.expiresAt(), friendScan.anchor, popularScan.anchor,
                                friendScan.exhausted, popularScan.exhausted);
                        return markSeen(viewer, ordered).thenReturn(new FeedResponse(viewer, size, ordered,
                                hasMore, hasMore ? codec.encode(next) : null));
                    })));
        });
    }

    private int scanBudget() { return Math.max(1, Math.min(400, properties.getMaxScanPerSource())); }
    private int batchSize(Scan state) {
        return Math.min(Math.max(1, Math.min(40, properties.getScanBatchSize())), scanBudget() - state.scanned);
    }

    private Mono<Void> scan(String viewer, MediaDisplayType media, Scan state, int quota,
                            Function<FeedSourceCursor, Mono<List<FeedCandidate>>> source, boolean popularSource,
                            Set<String> seen, Set<String> selectedIds, List<FeedItemResponse> selected) {
        return Mono.defer(() -> {
            if (quota <= 0 || state.exhausted || state.scanned >= scanBudget()) return Mono.empty();
            int requested = batchSize(state);
            // Recovery applies solely to Redis source reads. Hydration/DB errors must propagate.
            Mono<List<FeedCandidate>> batch = Mono.defer(() -> source.apply(state.anchor));
            if (popularSource) batch = batch.onErrorResume(error -> {
                state.unavailable = true;
                log.warn("|MixedFeedService|popular unavailable|viewer={}|error={}", viewer, error.getMessage());
                return Mono.empty();
            });
            return batch.flatMap(candidates -> {
                List<String> hydrateIds = candidates.stream()
                        .map(FeedCandidate::postId)
                        .filter(postId -> !seen.contains(postId) && !selectedIds.contains(postId))
                        .distinct()
                        .toList();
                return hydrator.hydratePage(viewer, hydrateIds, media)
                        .map(items -> items.stream().collect(java.util.stream.Collectors.toMap(
                                FeedItemResponse::postId, item -> item, (first, ignored) -> first)))
                        .flatMap(itemsById -> consume(state, quota, candidates, itemsById, 0,
                                popularSource, seen, selectedIds, selected)
                                .flatMap(lookahead -> {
                        if (lookahead) return Mono.empty();
                        if (candidates.size() < requested) state.exhausted = true;
                        return scan(viewer, media, state, quota, source, popularSource, seen, selectedIds, selected);
                                }));
            });
        });
    }

    private Mono<Boolean> consume(Scan state, int quota, List<FeedCandidate> batch,
                                  java.util.Map<String, FeedItemResponse> itemsById, int index, boolean popularSource,
                                  Set<String> seen, Set<String> selectedIds, List<FeedItemResponse> selected) {
        return Mono.defer(() -> {
            if (index >= batch.size()) return Mono.just(false);
            if (state.scanned >= scanBudget()) return Mono.just(true);
            FeedCandidate candidate = batch.get(index);
            state.scanned++;
            FeedItemResponse item = seen.contains(candidate.postId()) || selectedIds.contains(candidate.postId())
                    ? null : itemsById.get(candidate.postId());
            if (item != null && state.returned >= quota) return Mono.just(true);
            state.anchor = new FeedSourceCursor(candidate.sourceOrderTimeMillis(), candidate.postId(), candidate.sourceOrderTime());
            if (item != null) {
                selectedIds.add(candidate.postId());
                state.returned++;
                selected.add(item.withRecommendation(popularSource ? "POPULAR" : "FRIENDS",
                        popularSource ? "popular_post" : "friend_post", FeedCursorCodec.VERSION));
            }
            return consume(state, quota, batch, itemsById, index + 1, popularSource, seen, selectedIds, selected);
        });
    }

    private List<FeedItemResponse> interleave(List<FeedItemResponse> selected, int friendCount) {
        List<FeedItemResponse> ordered = new ArrayList<>(selected.size());
        int popularCount = selected.size() - friendCount;
        for (int index = 0; index < Math.max(friendCount, popularCount); index++) {
            if (index < friendCount) ordered.add(selected.get(index));
            if (index < popularCount) ordered.add(selected.get(friendCount + index));
        }
        return List.copyOf(ordered);
    }

    private Mono<Set<String>> loadSeen(String viewer) {
        return seenPosts.load(viewer).onErrorResume(error -> {
            log.warn("|MixedFeedService|seen read unavailable|viewer={}|error={}", viewer, error.getMessage());
            return Mono.just(new HashSet<>());
        });
    }
    private Mono<Void> markSeen(String viewer, List<FeedItemResponse> items) {
        if (items.isEmpty()) return Mono.empty();
        return seenPosts.mark(viewer, items.stream().map(FeedItemResponse::postId).toList())
                .onErrorResume(error -> {
                    log.warn("|MixedFeedService|seen write unavailable|viewer={}|error={}", viewer, error.getMessage());
                    return Mono.empty();
                });
    }
    private static final class Scan {
        FeedSourceCursor anchor;
        boolean exhausted;
        boolean unavailable;
        int scanned;
        int returned;
        Scan(FeedSourceCursor anchor, boolean exhausted) { this.anchor = anchor; this.exhausted = exhausted; }
    }
}
