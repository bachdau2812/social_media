package com.dauducbach.clone.modules.post.dto.event;

import java.time.Instant;

public record LikeEventPayload(
        String actorId,
        String targetId,
        String targetType,
        String targetOwnerId,
        String postId,
        String parentCommentId,
        Long likeCount,
        Instant timestamp
) {
}
