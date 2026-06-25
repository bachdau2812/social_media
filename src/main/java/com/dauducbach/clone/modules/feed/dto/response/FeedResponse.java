package com.dauducbach.clone.modules.feed.dto.response;

import java.util.List;

public record FeedResponse(
        String userId,
        int limit,
        List<FeedItemResponse> items,
        boolean hasMore
) {
}
