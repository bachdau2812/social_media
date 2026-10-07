package com.dauducbach.clone.modules.frontend.service;

import com.dauducbach.clone.modules.feed.publicapi.FeedScreenQuery;
import com.dauducbach.clone.modules.feed.publicapi.FeedScreenQuery.FeedSnapshot;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.publicapi.StoryTrayQuery;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.util.List;
import static org.mockito.Mockito.*;

class HomeScreenCursorTest {
    @Test void discoverPassesCursorToSharedFeedFlow() {
        FeedScreenQuery feed = mock(FeedScreenQuery.class);
        StoryTrayQuery stories = mock(StoryTrayQuery.class);
        when(stories.getHomeStoryTray("u")).thenReturn(Mono.just(List.of()));
        when(feed.getDiscoverFeed("u", 20, MediaDisplayType.FEED, "cursor"))
                .thenReturn(Mono.just(new FeedSnapshot("u", 20, List.of(), false, null)));
        new HomeScreenService(feed, stories).getHome("u", "DISCOVER", 20, 3, MediaDisplayType.FEED, "cursor").block();
        verify(feed).getDiscoverFeed("u", 20, MediaDisplayType.FEED, "cursor");
    }
    @Test void friendsKeepsPageAndRepostFlow() {
        FeedScreenQuery feed = mock(FeedScreenQuery.class);
        StoryTrayQuery stories = mock(StoryTrayQuery.class);
        when(stories.getHomeStoryTray("u")).thenReturn(Mono.just(List.of()));
        when(feed.getFriendsFeed("u", 20, 3, MediaDisplayType.FEED))
                .thenReturn(Mono.just(new FeedSnapshot("u", 20, List.of(), false, null)));
        new HomeScreenService(feed, stories).getHome("u", "FRIENDS", 20, 3, MediaDisplayType.FEED, null).block();
        verify(feed).getFriendsFeed("u", 20, 3, MediaDisplayType.FEED);
        verify(feed, never()).getDiscoverFeed(anyString(), anyInt(), any(), any());
    }
}
