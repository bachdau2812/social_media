package com.dauducbach.clone.modules.frontend.dto;

import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;

import java.time.Instant;
import java.util.List;

public record ProfilePostResponse(
        String postId,
        String userId,
        String authorUsername,
        String authorFullName,
        String authorAvatarUrl,
        String content,
        List<String> hashtags,
        String mediaRatio,
        PostItemResponse firstItem,
        PostMusicResponse music,
        long likeCount,
        long commentCount,
        long repostCount,
        boolean likedByCurrentUser,
        boolean repostedByCurrentUser,
        Instant createdAt,
        Instant updatedAt
) {
}