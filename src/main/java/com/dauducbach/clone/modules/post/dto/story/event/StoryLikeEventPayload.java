package com.dauducbach.clone.modules.post.dto.story.event;

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
