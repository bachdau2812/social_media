package com.dauducbach.clone.modules.post.publicapi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

public interface PostQuery {
    Mono<PostSnapshot> findSnapshot(String postId);

    /** Approved, non-archived post content for feed pages. Results need not follow the input order. */
    Flux<FeedPostSnapshot> findApprovedFeedSnapshots(Collection<String> postIds);

    record PostSnapshot(String postId, String ownerId, String content) { }

    record FeedPostSnapshot(
            String postId,
            String ownerId,
            String content,
            String hashtag,
            List<String> hashtags,
            String mediaRatio,
            String validateStatus,
            String musicId,
            Long musicStart,
            Long musicEnd,
            Instant createdAt,
            Instant updatedAt,
            List<FeedItemSnapshot> items) {
        public FeedPostSnapshot {
            hashtags = hashtags == null ? List.of() : List.copyOf(hashtags);
            items = items == null ? List.of() : List.copyOf(items);
        }
    }

    record FeedItemSnapshot(
            String id,
            Integer orderNumber,
            String caption,
            String mediaId,
            String musicId,
            Long musicStart,
            Long musicEnd) { }
}
