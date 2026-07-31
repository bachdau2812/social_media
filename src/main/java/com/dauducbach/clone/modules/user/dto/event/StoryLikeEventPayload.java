package com.dauducbach.clone.modules.user.dto.event;

import java.time.Instant;

public record StoryLikeEventPayload(
        String actorId,
        String targetId,
        String targetType,
        String targetOwnerId,
        String interactionId,
        Instant timestamp
) {
}
