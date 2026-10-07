package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import org.junit.jupiter.api.Test;
import com.dauducbach.clone.modules.post.publicapi.PostFeedQuery;
import reactor.core.publisher.Mono;
import java.util.List;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

class MixedFeedRoutingTest {
    private FeedService feed() {
        return feed(mock(MixedFeedService.class), new PostPopularityProperties());
    }

    private FeedService feed(MixedFeedService mixed, PostPopularityProperties properties) {
        return new FeedService(mock(FeedSeenPostStore.class), mock(PostFeedQuery.class),
                mock(FeedCandidatePipeline.class), mock(FeedItemHydrator.class),
                mock(FeedVectorSnapshotService.class), mock(FeedQueue.class),
                mixed, properties);
    }
    @Test void enabledFlagDelegatesBothOverloadsToSameMixedService() {
        var mixed = mock(MixedFeedService.class);
        var properties = new PostPopularityProperties();
        properties.setMixedFeedEnabled(true);
        var service = feed(mixed, properties);
        when(mixed.getFeed("u", 20, MediaDisplayType.FEED, null)).thenReturn(Mono.just(new FeedResponse("u", 20, List.of(), false)));
        when(mixed.getFeed("u", 50, MediaDisplayType.FEED, "cursor")).thenReturn(Mono.just(new FeedResponse("u", 50, List.of(), false)));
        service.getFeed("u", 0, null).block();
        service.getFeed("u", 100, null, "cursor").block();
        verify(mixed).getFeed("u", 20, MediaDisplayType.FEED, null);
        verify(mixed).getFeed("u", 50, MediaDisplayType.FEED, "cursor");
    }
    @Test void disabledFlagRejectsUnexpectedCursor() {
        var service = feed(mock(MixedFeedService.class), new PostPopularityProperties());
        assertThrows(AppException.class, () -> service.getFeed("u", 20, MediaDisplayType.FEED, "cursor").block());
    }
}
