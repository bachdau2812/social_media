package com.dauducbach.clone.modules.post.dto.response;

import java.time.Instant;

public record FriendFeedActivityResponse(
        String feedEntryId,
        String postId,
        String activityType,
        String actorId,
        Instant activityAt
) {
}
