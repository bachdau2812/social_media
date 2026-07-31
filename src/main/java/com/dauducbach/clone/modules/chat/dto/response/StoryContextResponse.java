package com.dauducbach.clone.modules.chat.dto.response;

import java.time.Instant;

public record StoryContextResponse(
        String storyId,
        String storyOwnerId,
        String mediaType,
        Long previewAtMs,
        Instant expiresAt,
        Boolean available,
        String previewUrl) {
}
