package com.dauducbach.clone.modules.feed.dto.response;

import java.util.List;

public record FeedResponse(
        String userId,
        int limit,
        List<FeedItemResponse> items,
        boolean hasMore,
        String nextCursor
 ) {
    public FeedResponse(String userId, int limit, List<FeedItemResponse> items, boolean hasMore) {
        this(userId, limit, items, hasMore, null);
    }

}
