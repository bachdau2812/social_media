package com.dauducbach.clone.modules.frontend.service;

import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.feed.service.FeedService;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.service.story.StoryTrayQueryService;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.util.List;
import static org.mockito.Mockito.*;

class HomeScreenCursorTest {
    @Test void discoverPassesCursorToSharedFeedFlow() {
        FeedService feed = mock(FeedService.class);
        StoryTrayQueryService stories = mock(StoryTrayQueryService.class);
        when(stories.getHomeStoryTray("u")).thenReturn(Mono.just(List.of()));
        when(feed.getFeed("u", 20, MediaDisplayType.FEED, "cursor")).thenReturn(Mono.just(new FeedResponse("u", 20, List.of(), false)));
        new HomeScreenService(feed, stories).getHome("u", "DISCOVER", 20, 3, MediaDisplayType.FEED, "cursor").block();
        verify(feed).getFeed("u", 20, MediaDisplayType.FEED, "cursor");
    }
    @Test void friendsKeepsPageAndRepostFlow() {
        FeedService feed = mock(FeedService.class);
        StoryTrayQueryService stories = mock(StoryTrayQueryService.class);
        when(stories.getHomeStoryTray("u")).thenReturn(Mono.just(List.of()));
        when(feed.getFriendsFeed("u", 20, 3, MediaDisplayType.FEED)).thenReturn(Mono.just(new FeedResponse("u", 20, List.of(), false)));
        new HomeScreenService(feed, stories).getHome("u", "FRIENDS", 20, 3, MediaDisplayType.FEED, null).block();
        verify(feed).getFriendsFeed("u", 20, 3, MediaDisplayType.FEED);
        verify(feed, never()).getFeed(anyString(), anyInt(), any(), any());
    }
}
