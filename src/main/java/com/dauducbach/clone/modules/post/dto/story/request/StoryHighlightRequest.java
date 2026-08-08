package com.dauducbach.clone.modules.post.dto.story.request;

import java.util.List;

public record StoryHighlightRequest(
        String ownerId,
        String title,
        String coverStoryId,
        List<String> storyIds
) {
}
