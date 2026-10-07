package com.dauducbach.clone.modules.post.publicapi;

import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.List;

/** Read contract for profile-owned and reposted post summaries. */
public interface PostProfileQuery {
    Flux<ProfilePostSnapshot> getRecentPosts(String viewerId, String userId, int limit);

    Flux<ProfilePostSnapshot> getRepostedPosts(String viewerId, String userId, int limit);

    record ProfilePostSnapshot(
            String postId,
            String userId,
            String authorUsername,
            String authorFullName,
            String authorAvatarUrl,
            String content,
            List<String> hashtags,
            String mediaRatio,
            PostPresentationSnapshot.Item firstItem,
            PostPresentationSnapshot.Music music,
            long likeCount,
            long commentCount,
            long repostCount,
            boolean likedByCurrentUser,
            boolean repostedByCurrentUser,
            Instant createdAt,
            Instant updatedAt
    ) {
        public ProfilePostSnapshot {
            hashtags = hashtags == null ? List.of() : List.copyOf(hashtags);
        }
    }
}
