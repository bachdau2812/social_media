package com.dauducbach.clone.modules.user.dto.response;

import java.time.Instant;

public record StoryTrayResponse(
        String storyId,
        String userId,
        String username,
        String fullName,
        String avatarUrl,
        String mediaUrl,
        String mediaType,
        String musicId,
        String musicUrl,
        String musicDisplayName,
        Long musicStart,
        Long musicEnd,
        Long durationSeconds,
        String status,
        Instant createdAt,
        Instant expiredAt,
        String publicationId,
        Integer publicationOrder,
        Integer publicationItemCount,
        boolean viewerSeen
) {
}