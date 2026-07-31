package com.dauducbach.clone.modules.chat.dto.request;

import java.time.Instant;

public record StoryContextRequest(
        String storyId,
        String storyOwnerId,
        String mediaType,
        Long previewAtMs,
        Instant expiresAt) {
}
