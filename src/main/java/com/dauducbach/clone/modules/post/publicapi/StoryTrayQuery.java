package com.dauducbach.clone.modules.post.publicapi;

import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

public interface StoryTrayQuery {
    Mono<List<StoryTraySnapshot>> getHomeStoryTray(String viewerId);

    record StoryTraySnapshot(
            String storyId,
            String userId,
            String username,
            String fullName,
            String avatarUrl,
            String mediaUrl,
            String mediaType,
            String musicId,
            String musicUrl,
            String musicDisplayName,
            Long musicStart,
            Long musicEnd,
            Long durationSeconds,
            String status,
            Instant createdAt,
            Instant expiredAt,
            String publicationId,
            Integer publicationOrder,
            Integer publicationItemCount,
            boolean viewerSeen,
            String viewerReaction
    ) {
    }
}
