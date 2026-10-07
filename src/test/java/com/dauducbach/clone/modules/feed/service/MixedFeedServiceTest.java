package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MixedFeedServiceTest {
    FriendFeedCandidateSource friends = mock(FriendFeedCandidateSource.class);
    PopularFeedCandidateSource popular = mock(PopularFeedCandidateSource.class);
    FeedItemHydrator hydrator = mock(FeedItemHydrator.class);
    FeedSeenPostStore seenPosts = mock(FeedSeenPostStore.class);
    PostPopularityProperties properties = new PostPopularityProperties();
    MixedFeedService service;
    @BeforeEach void setup() {
        properties.setCursorSecret("01234567890123456789012345678901");
        service = new MixedFeedService(friends, popular, hydrator, seenPosts, new FeedCursorCodec(properties), properties);
        when(seenPosts.load(anyString())).thenReturn(Mono.just(java.util.Set.of()));
        when(seenPosts.mark(anyString(), anyList())).thenReturn(Mono.empty());
        when(hydrator.hydratePage(anyString(), anyList(), any())).thenAnswer(call -> {
            List<String> ids = call.getArgument(1);
            return Mono.just(ids.stream().map(MixedFeedServiceTest::item).toList());
        });
    }
    @Test void strictQuotasDoNotBorrowFromPopularWhenFriendsShort() {
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("f", 8)));
        when(popular.find(any(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("p", 30)));
        var result = service.getFeed("u", 20, MediaDisplayType.FEED, null).block();
        assertEquals(18, result.items().size());
        assertEquals(8, result.items().stream().filter(i -> "FRIENDS".equals(i.sourceType())).count());
        assertEquals(10, result.items().stream().filter(i -> "POPULAR".equals(i.sourceType())).count());
        assertTrue(result.hasMore());
    }
    @Test void fullPageReturnsTenFriendsAndTenPopularPosts() {
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("f", 15)));
        when(popular.find(any(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("p", 15)));
        var result = service.getFeed("u", 20, MediaDisplayType.POST, null).block();
        assertEquals(20, result.items().size());
        assertEquals(10, result.items().stream().filter(i -> "FRIENDS".equals(i.sourceType())).count());
        assertEquals(10, result.items().stream().filter(i -> "POPULAR".equals(i.sourceType())).count());
        verify(hydrator, atLeastOnce()).hydratePage(eq("u"), anyList(), eq(MediaDisplayType.POST));
        assertEquals(IntStream.range(0, 10).boxed().flatMap(i -> java.util.stream.Stream.of("f" + i, "p" + i)).toList(),
                result.items().stream().map(FeedItemResponse::postId).toList());
    }
    @Test void oddLimitGivesFriendsExtraAndDeduplicatesOverlap() {
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("f", 3)));
        when(popular.find(any(), any(), any(), anyInt())).thenReturn(Mono.just(List.of(candidate("f0", 9), candidate("p0", 8), candidate("p1", 7))));
        var result = service.getFeed("u", 5, MediaDisplayType.FEED, null).block();
        assertEquals(List.of("f0", "p0", "f1", "p1", "f2"), result.items().stream().map(FeedItemResponse::postId).toList());
    }
    @Test void zeroPopularQuotaDoesNotCauseEndlessEmptyPages() {
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(List.of()));
        when(popular.find(any(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("p", 3)));
        var result = service.getFeed("u", 1, MediaDisplayType.FEED, null).block();
        assertTrue(result.items().isEmpty());
        assertFalse(result.hasMore());
        assertNull(result.nextCursor());
        verifyNoInteractions(popular);
    }
    @Test void popularOutageKeepsFriendsQuotaAndRetryableCursor() {
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("f", 3)));
        when(popular.find(any(), any(), any(), anyInt())).thenReturn(Mono.error(new IllegalStateException("redis offline")));
        var result = service.getFeed("u", 4, MediaDisplayType.FEED, null).block();
        assertEquals(2, result.items().size());
        assertTrue(result.hasMore());
        assertNotNull(result.nextCursor());
    }
    @Test void latePopularOutageRollsBackPartialSelectionForRetry() {
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("f", 3)));
        when(popular.find(any(), any(), any(), anyInt())).thenAnswer(call -> call.getArgument(2) == null
                ? Mono.just(candidates("p", 40)) : Mono.error(new IllegalStateException("redis offline")));
        when(hydrator.hydratePage(eq("u"), anyList(), any())).thenAnswer(call -> {
            List<String> ids = call.getArgument(1);
            return Mono.just(ids.stream().filter(id -> !id.startsWith("p") || "p0".equals(id))
                    .map(MixedFeedServiceTest::item).toList());
        });
        var result = service.getFeed("u", 4, MediaDisplayType.FEED, null).block();
        assertEquals(List.of("f0", "f1"), result.items().stream().map(FeedItemResponse::postId).toList());
        var state = new FeedCursorCodec(properties).decode(result.nextCursor(), "u", MediaDisplayType.FEED, Instant.now());
        assertNull(state.popular());
        assertFalse(state.popularExhausted());
        verify(seenPosts).mark("u", List.of("f0", "f1"));
    }
    @Test void eligibilityFailurePropagates() {
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("f", 1)));
        when(hydrator.hydratePage(anyString(), anyList(), any()))
                .thenReturn(Mono.error(new IllegalStateException("database unavailable")));
        assertThrows(IllegalStateException.class, () -> service.getFeed("u", 2, MediaDisplayType.FEED, null).block());
    }
    @Test void scanBudgetCarriesProgressPastFourHundredRejectedPosts() {
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(List.of()));
        when(popular.find(any(), any(), any(), anyInt())).thenAnswer(call -> {
            var after = (com.dauducbach.clone.modules.feed.dto.FeedSourceCursor) call.getArgument(2);
            int start = after == null ? 0 : Integer.parseInt(after.postId().substring(1)) + 1;
            int count = call.getArgument(3);
            return Mono.just(IntStream.range(start, Math.min(start + count, 405))
                    .mapToObj(i -> candidate("p" + i, 1000 - i)).toList());
        });
        when(hydrator.hydratePage(anyString(), anyList(), any())).thenAnswer(call -> {
            List<String> ids = call.getArgument(1);
            return Mono.just(ids.stream()
                    .filter(id -> Integer.parseInt(id.substring(1)) >= 400)
                    .map(MixedFeedServiceTest::item).toList());
        });
        var first = service.getFeed("u", 2, MediaDisplayType.FEED, null).block();
        assertTrue(first.items().isEmpty());
        assertTrue(first.hasMore());
        var second = service.getFeed("u", 2, MediaDisplayType.FEED, first.nextCursor()).block();
        assertEquals("p400", second.items().getFirst().postId());
    }
    @Test void validLookaheadRemainsAvailableOnNextPage() {
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(List.of()));
        when(popular.find(any(), any(), any(), anyInt())).thenAnswer(call -> {
            var after = (com.dauducbach.clone.modules.feed.dto.FeedSourceCursor) call.getArgument(2);
            return Mono.just(after == null ? List.of(candidate("p0", 10), candidate("p1", 9)) : List.of(candidate("p1", 9)));
        });
        var first = service.getFeed("u", 2, MediaDisplayType.FEED, null).block();
        assertEquals("p0", first.items().getFirst().postId());
        var second = service.getFeed("u", 2, MediaDisplayType.FEED, first.nextCursor()).block();
        assertEquals("p1", second.items().getFirst().postId());
    }
    @Test void filtersSeenAndDeletedWithoutMarkingThemSeenAgain() {
        when(seenPosts.load("u")).thenReturn(Mono.just(java.util.Set.of("f0")));
        when(friends.find(anyString(), any(), any(), anyInt())).thenReturn(Mono.just(candidates("f", 4)));
        when(popular.find(any(), any(), any(), anyInt())).thenReturn(Mono.just(List.of()));
        when(hydrator.hydratePage(eq("u"), anyList(), eq(MediaDisplayType.FEED))).thenAnswer(call -> {
            List<String> ids = call.getArgument(1);
            return Mono.just(ids.stream().filter(id -> !"f1".equals(id)).map(MixedFeedServiceTest::item).toList());
        });
        var result = service.getFeed("u", 4, MediaDisplayType.FEED, null).block();
        assertEquals(List.of("f2", "f3"), result.items().stream().map(FeedItemResponse::postId).toList());
        verify(seenPosts).mark("u", List.of("f2", "f3"));
    }
    static List<FeedCandidate> candidates(String prefix, int size) {
        return IntStream.range(0, size).mapToObj(i -> candidate(prefix + i, 1000 - i)).toList();
    }
    static FeedCandidate candidate(String id, long time) { return new FeedCandidate(id, "unused", "unused", 0, time); }
    static FeedItemResponse item(String id) {
        return new FeedItemResponse(id, "author", "name", "name", "", "", List.of(), "1:1", List.of(), null, List.of(), 0, 0, 0, false, false, Instant.EPOCH, Instant.EPOCH);
    }
}
