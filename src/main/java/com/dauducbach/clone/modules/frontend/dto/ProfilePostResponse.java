package com.dauducbach.clone.modules.frontend.dto;

import com.dauducbach.clone.modules.post.publicapi.PostPresentationSnapshot;

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
}
