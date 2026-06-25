package com.dauducbach.clone.modules.feed.dto.response;

import java.time.Instant;

public record FeedLongTermVectorRefreshResponse(
        String userId,
        Instant from,
        Instant to,
        Instant refreshedAt,
        String status
) {
}
