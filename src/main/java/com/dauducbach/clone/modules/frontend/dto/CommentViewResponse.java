package com.dauducbach.clone.modules.frontend.dto;

import java.time.Instant;

public record CommentViewResponse(
        String id,
        String postId,
        String userId,
        String parentId,
        String content,
        String commentType,
        String mediaUrl,
        Instant timestamp,
        long replyCount,
        boolean hasLiked,
        String username,
        String fullName,
        String avatarUrl
) {
}