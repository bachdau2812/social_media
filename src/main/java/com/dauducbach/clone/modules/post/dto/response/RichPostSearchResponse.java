package com.dauducbach.clone.modules.post.dto.response;

import java.time.Instant;
import java.util.List;

public record RichPostSearchResponse(
        String postId,
        String userId,
        String authorUsername,
        String authorFullName,
        String authorAvatarUrl,
        String content,
        List<String> hashtags,
        String mediaRatio,
        List<PostItemResponse> items,
        int totalMediaItems,
        Instant createdAt
) {
    public RichPostSearchResponse {
        hashtags = hashtags == null ? List.of() : List.copyOf(hashtags);
        items = items == null ? List.of() : List.copyOf(items);
    }
}
