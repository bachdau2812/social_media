package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.constant.FeedActivityType;
import com.dauducbach.clone.modules.feed.dto.response.FeedActorResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedMediaResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMediaResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.ObjectMapper;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FeedScreenQueryServiceTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void mapsOwnedFeedResponseToStableScreenSnapshot() {
        FeedService feed = mock(FeedService.class);
        PostMusicResponse music = new PostMusicResponse("music", "Track", "Artist", "art", "play", 1L, 8L, 9L);
        PostItemResponse postItem = new PostItemResponse("item", 0, "caption",
                new PostMediaResponse("asset", "public", "jpg", "image", "url", "secure", "photo", 640, 480),
                music);
        FeedItemResponse item = new FeedItemResponse(
                "post", "author", "alice", "Alice", "avatar", "hello", List.of("tag"), "4:3",
                List.of(new FeedMediaResponse("asset", "public", "jpg", "image", "url", "secure", "photo")),
                music, List.of(postItem), 3, 4, 5, true, false, null, null,
                "MIXED", "because", "v1", "experiment", "impression", "entry",
                FeedActivityType.REPOST, null, new FeedActorResponse("actor", "bob", "Bob", "avatar-2"));
        FeedResponse legacy = new FeedResponse("viewer", 15, List.of(item), true, "cursor-2");
        when(feed.getFeed("viewer", 15, MediaDisplayType.FEED, "next")).thenReturn(Mono.just(legacy));

        var snapshot = new FeedScreenQueryService(feed)
                .getDiscoverFeed("viewer", 15, MediaDisplayType.FEED, "next")
                .block();

        assertEquals("cursor-2", snapshot.nextCursor());
        assertEquals(1, snapshot.items().size());
        var mapped = snapshot.items().getFirst();
        assertEquals("post", mapped.postId());
        assertEquals("4:3", mapped.mediaRatio());
        assertEquals("REPOST", mapped.activityType());
        assertEquals("actor", mapped.reposter().id());
        assertEquals("asset", mapped.items().getFirst().media().assetId());
        assertEquals("music", mapped.music().id());
        assertEquals(objectMapper.convertValue(legacy, java.util.Map.class),
                objectMapper.convertValue(snapshot, java.util.Map.class));
        verify(feed).getFeed("viewer", 15, MediaDisplayType.FEED, "next");
    }
}
