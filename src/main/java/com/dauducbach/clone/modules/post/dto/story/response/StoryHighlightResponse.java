package com.dauducbach.clone.modules.post.dto.story.response;

import com.dauducbach.clone.modules.post.entity.story.UserStories;

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
