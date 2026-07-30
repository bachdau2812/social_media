package com.dauducbach.clone.modules.user.dto.request;

import java.util.List;

public record StoryHighlightRequest(
        String ownerId,
        String title,
        String coverStoryId,
        List<String> storyIds
) {
}
