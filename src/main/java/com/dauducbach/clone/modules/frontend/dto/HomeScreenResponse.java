package com.dauducbach.clone.modules.frontend.dto;

import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import java.util.List;

public record HomeScreenResponse(
        String activeTab,
        List<HomeTab> tabs,
        List<StoryTrayItemResponse> storyTray,
        FeedResponse feed,
        List<String> suggestedUsers
) {
    public record HomeTab(String id, String label, long unreadCount) {
    }
}