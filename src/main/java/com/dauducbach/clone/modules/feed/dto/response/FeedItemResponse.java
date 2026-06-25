package com.dauducbach.clone.modules.feed.dto.response;

import java.time.Instant;
import java.util.List;

public record FeedItemResponse(
        String postId,
        String userId,
        String authorUsername,
        String content,
        List<String> hashtags,
        List<FeedMediaResponse> media,
        long likeCount,
        long commentCount,
        boolean likedByCurrentUser,
        Instant createdAt,
        Instant updatedAt
) {
}
