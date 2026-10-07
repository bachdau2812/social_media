package com.dauducbach.clone.modules.post.repository.story.projection;

import java.time.Instant;

public record StoryViewerRow(String viewerId, String reaction, Instant viewedAt) {
}
