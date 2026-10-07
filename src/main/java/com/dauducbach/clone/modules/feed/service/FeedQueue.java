package com.dauducbach.clone.modules.feed.service;

import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/** Feed's persistence boundary for candidate delivery and fanout state. */
public interface FeedQueue {
    Mono<QueueRead> readForServe(String viewerId, int limit, Set<String> excludedPostIds);

    Mono<Boolean> commit(String viewerId, long vectorVersion, List<FeedCandidate> candidates);

    Mono<Void> appendFanout(String viewerId, String postId, Instant eventTime);

    Mono<Void> removeReturned(String viewerId, List<String> postIds);

    Mono<Boolean> hasMore(String viewerId);

    record QueueRead(List<String> postIds, boolean needsRefill, boolean legacy) {
        public QueueRead { postIds = List.copyOf(postIds); }
    }
}
