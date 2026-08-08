package com.dauducbach.clone.modules.post.dto.story.response;

import java.time.Instant;

public record StoryViewerResponse(
        String userId,
        String username,
        String fullName,
        String avatarUrl,
        String reaction,
        Instant viewedAt
) {
}
