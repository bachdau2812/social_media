package com.dauducbach.clone.modules.frontend.dto;

import com.dauducbach.clone.modules.feed.publicapi.FeedScreenQuery.FeedSnapshot;
import java.util.List;

public record HomeScreenResponse(
        String activeTab,
        List<HomeTab> tabs,
        List<StoryTrayItemResponse> storyTray,
        FeedSnapshot feed,
        List<String> suggestedUsers
) {
    public record HomeTab(String id, String label, long unreadCount) {
    }
}
