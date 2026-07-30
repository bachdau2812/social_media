package com.dauducbach.clone.modules.post.dto.response;

import java.time.Instant;
import java.util.List;

public record PostDetailResponse(
        String postId,
        String userId,
        String authorUsername,
        String authorFullName,
        String content,
        String hashtag,
        List<String> hashtags,
        String mediaRatio,
        String validateStatus,
        String musicId,
        Long musicStart,
        Long musicEnd,
        PostMusicResponse music,
        List<PostItemResponse> items,
        Instant createdAt,
        Instant updatedAt
) {
}
