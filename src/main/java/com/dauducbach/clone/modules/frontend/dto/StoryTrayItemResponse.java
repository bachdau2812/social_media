package com.dauducbach.clone.modules.frontend.dto;

import java.time.Instant;

public record StoryTrayItemResponse(
        String id,
        String userId,
        String username,
        String fullName,
        String avatarUrl,
        String mediaUrl,
        String mediaType,
        String musicId,
        String musicUrl,
        String musicName,
        Long musicStart,
        Long musicEnd,
        Long durationSeconds,
        String status,
        Instant createdAt,
        Instant expiredAt,
        String publicationId,
        Integer publicationOrder,
        Integer publicationItemCount,
        Boolean viewerSeen
) {
}
