package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.feed.constant.FeedActivityType;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.dto.response.FriendFeedActivityResponse;
import com.dauducbach.clone.modules.post.service.PostFeedQueryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
@ExtendWith(MockitoExtension.class)
class FeedServiceTest {
    @Mock
    ReactiveRedisTemplate<String, String> redisTemplate;
    @Mock
    ReactiveZSetOperations<String, String> zSetOperations;
    @Mock
    PostFeedQueryService postFeedQueryService;
    @Mock
    FeedCandidatePipeline candidatePipeline;
    @Mock
    FeedItemHydrator itemHydrator;

    @Test
    void friendsFeedKeepsDistinctRepostActivitiesAndCalculatesHasMoreFromActivities() {
        FeedService service = newService();
        FriendFeedActivityResponse first = activity("repost-1", "post-1", "friend-1", 3);
        FriendFeedActivityResponse second = activity("repost-2", "post-1", "friend-2", 2);
        FriendFeedActivityResponse third = activity("repost-3", "post-2", "friend-1", 1);

        when(postFeedQueryService.getRecentFriendFeedActivities("viewer-1", 3, 0))
                .thenReturn(Flux.just(first, second, third));
        when(itemHydrator.hydrateFriendActivity("viewer-1", first, MediaDisplayType.FEED))
                .thenReturn(Mono.just(feedItem("post-1").withActivity(
                        "repost-1", FeedActivityType.REPOST, first.activityAt(), null)));
        when(itemHydrator.hydrateFriendActivity("viewer-1", second, MediaDisplayType.FEED))
                .thenReturn(Mono.just(feedItem("post-1").withActivity(
                        "repost-2", FeedActivityType.REPOST, second.activityAt(), null)));

        StepVerifier.create(service.getFriendsFeed("viewer-1", 2, 0, MediaDisplayType.FEED))
                .assertNext(response -> {
                    org.junit.jupiter.api.Assertions.assertTrue(response.hasMore());
                    org.junit.jupiter.api.Assertions.assertEquals(
                            List.of("repost-1", "repost-2"),
                            response.items().stream().map(FeedItemResponse::feedEntryId).toList()
                    );
                    org.junit.jupiter.api.Assertions.assertEquals(
                            List.of("post-1", "post-1"),
                            response.items().stream().map(FeedItemResponse::postId).toList()
                    );
                })
                .verifyComplete();
    }

    @Test
    void appendPostToUserFeedWritesZSetWithTtl() {
        FeedService service = newService();

        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.add("feed:user-1", "post-1", Instant.parse("2026-06-22T00:00:00Z").toEpochMilli()))
                .thenReturn(Mono.just(true));
        when(redisTemplate.expire("feed:user-1", Duration.ofDays(5))).thenReturn(Mono.just(true));

        StepVerifier.create(service.appendPostToUserFeed("user-1", "post-1", Instant.parse("2026-06-22T00:00:00Z")))
                .verifyComplete();

        verify(zSetOperations).add("feed:user-1", "post-1", Instant.parse("2026-06-22T00:00:00Z").toEpochMilli());
    }

    @Test
    void getFeedRejectsBlankUserId() {
        FeedService service = newService();

        StepVerifier.create(Mono.defer(() -> service.getFeed(" ", 20)))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.POST_LIST_FETCH_FAILED)
                .verify();
    }

    private FeedService newService() {
        return new FeedService(
                redisTemplate,
                postFeedQueryService,
                candidatePipeline,
                itemHydrator
        );
    }

    private FriendFeedActivityResponse activity(
            String feedEntryId,
            String postId,
            String actorId,
            long seconds
    ) {
        return new FriendFeedActivityResponse(
                feedEntryId,
                postId,
                "REPOST",
                actorId,
                Instant.parse("2026-07-31T00:00:00Z").plusSeconds(seconds)
        );
    }

    private FeedItemResponse feedItem(String postId) {
        Instant now = Instant.parse("2026-07-31T00:00:00Z");
        return new FeedItemResponse(
                postId, "author-1", "author", "Author", "", "content",
                List.of(), "1:1", List.of(), null, List.of(),
                0, 0, 0, false, false, now, now
        );
    }
}
