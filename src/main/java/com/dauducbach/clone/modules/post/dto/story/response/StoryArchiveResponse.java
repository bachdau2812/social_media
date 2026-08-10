package com.dauducbach.clone.modules.post.dto.story.response;

import java.time.Instant;

public record StoryArchiveResponse(
        String id,
        String userId,
        String mediaUrl,
        String mediaType,
        String musicId,
        String musicUrl,
        String musicName,
        Long musicStart,
        Long musicEnd,
        Long durationSeconds,
        String publicationId,
        Integer publicationOrder,
        Integer publicationItemCount,
        String status,
        Instant createdAt,
        Instant expiredAt,
        Boolean viewerSeen
) {
}
