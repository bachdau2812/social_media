package com.dauducbach.clone.modules.user.dto.response;

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
