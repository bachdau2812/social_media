package com.dauducbach.clone.modules.user.dto.response;

import com.dauducbach.clone.modules.user.entity.UserStories;

import java.time.Instant;
import java.util.List;

public record StoryHighlightResponse(
        String id,
        String ownerId,
        String title,
        String coverStoryId,
        String coverUrl,
        Instant createdAt,
        Instant updatedAt,
        List<UserStories> stories
) {
}
