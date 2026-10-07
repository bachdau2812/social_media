package com.dauducbach.clone.modules.feed.publicapi;

import com.dauducbach.clone.modules.post.publicapi.PostPresentationSnapshot;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

/** Read contract for the home screen feed. */
public interface FeedScreenQuery {
    Mono<FeedSnapshot> getDiscoverFeed(String userId, int limit, MediaDisplayType mediaType, String cursor);

    Mono<FeedSnapshot> getFriendsFeed(String userId, int limit, int page, MediaDisplayType mediaType);

    record FeedSnapshot(String userId, int limit, List<FeedItemSnapshot> items, boolean hasMore, String nextCursor) {
        public FeedSnapshot {
            items = items == null ? List.of() : List.copyOf(items);
        }
    }

    record FeedItemSnapshot(
            String postId,
            String userId,
            String authorUsername,
            String authorFullName,
            String authorAvatarUrl,
            String content,
            List<String> hashtags,
            String mediaRatio,
            List<FeedMediaSnapshot> media,
            PostPresentationSnapshot.Music music,
            List<PostPresentationSnapshot.Item> items,
            long likeCount,
            long commentCount,
            long repostCount,
            boolean likedByCurrentUser,
            boolean repostedByCurrentUser,
            Instant createdAt,
            Instant updatedAt,
            String sourceType,
            String recommendationReason,
            String rankingVersion,
            String experimentId,
            String impressionToken,
            String feedEntryId,
            String activityType,
            Instant activityAt,
            FeedActorSnapshot reposter
    ) {
        public FeedItemSnapshot {
            hashtags = hashtags == null ? List.of() : List.copyOf(hashtags);
            media = media == null ? List.of() : List.copyOf(media);
            items = items == null ? List.of() : List.copyOf(items);
        }
    }

    record FeedMediaSnapshot(
            String assetId,
            String publicId,
            String mediaFormat,
            String resourceType,
            String url,
            String secureUrl,
            String displayName
    ) {
    }

    record FeedActorSnapshot(String id, String username, String displayName, String avatarUrl) {
    }
}
