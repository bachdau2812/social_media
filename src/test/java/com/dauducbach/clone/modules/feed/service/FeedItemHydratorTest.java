package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.constant.FeedActivityType;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.dto.response.FriendFeedActivityResponse;
import com.dauducbach.clone.modules.post.service.comment.CommentService;
import com.dauducbach.clone.modules.post.service.post.LikeService;
import com.dauducbach.clone.modules.post.service.post.PostDetailQueryService;
import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import com.dauducbach.clone.modules.post.service.post.RepostService;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.MediaForProfile;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FeedItemHydratorTest {
    @Mock ReactiveRedisTemplate<String, String> redisTemplate;
    @Mock PostFeedQueryService postFeedQueryService;
    @Mock PostDetailQueryService postDetailQueryService;
    @Mock MediaCompatibilityFacade mediaFacade;
    @Mock MediaService mediaService;
    @Mock MediaForProfile mediaForProfile;
    @Mock UserDetailsService userDetailsService;
    @Mock LikeService likeService;
    @Mock CommentService commentService;
    @Mock RepostService repostService;

    @InjectMocks
    FeedItemHydrator hydrator;

    @Test
    void repostActivityUsesReposterIdentityWithoutChangingOriginalPostIdentity() {
        FeedItemHydrator activityHydrator = spy(new FeedItemHydrator(
                redisTemplate, postFeedQueryService, postDetailQueryService, mediaFacade,
                mediaService, mediaForProfile, userDetailsService, likeService, commentService, repostService
        ));
        Instant activityAt = Instant.parse("2026-07-31T00:00:00Z");
        FriendFeedActivityResponse activity = new FriendFeedActivityResponse(
                "repost-1", "post-1", "REPOST", "friend-1", activityAt
        );
        doReturn(Mono.just(feedItem())).when(activityHydrator)
                .hydrate("viewer-1", "post-1", MediaDisplayType.FEED);
        when(userDetailsService.getUserDetailsById("friend-1")).thenReturn(Mono.just(
                UserDetails.builder().userId("friend-1").username("an").fullName("An").build()
        ));
        when(mediaForProfile.getCurrentAvatar("friend-1", MediaDisplayType.AVATAR)).thenReturn(Mono.just(
                Media.builder().assetId("avatar-1").secureUrl("https://cdn.example/an.jpg").build()
        ));

        StepVerifier.create(activityHydrator.hydrateFriendActivity(
                        "viewer-1", activity, MediaDisplayType.FEED))
                .assertNext(item -> {
                    org.junit.jupiter.api.Assertions.assertEquals("post-1", item.postId());
                    org.junit.jupiter.api.Assertions.assertEquals("repost-1", item.feedEntryId());
                    org.junit.jupiter.api.Assertions.assertEquals(FeedActivityType.REPOST, item.activityType());
                    org.junit.jupiter.api.Assertions.assertEquals(activityAt, item.activityAt());
                    org.junit.jupiter.api.Assertions.assertEquals("friend-1", item.reposter().id());
                    org.junit.jupiter.api.Assertions.assertEquals("An", item.reposter().displayName());
                    org.junit.jupiter.api.Assertions.assertEquals(
                            "https://cdn.example/an.jpg", item.reposter().avatarUrl());
                })
                .verifyComplete();
    }

    @Test
    void rejectedArchivedOrDeletedPostStopsBeforeCacheHydration() {
        when(postFeedQueryService.getApprovedPostById("post-1")).thenReturn(Mono.empty());

        StepVerifier.create(hydrator.hydrate("viewer-1", "post-1", null))
                .verifyComplete();

        verifyNoInteractions(redisTemplate);
    }

    private FeedItemResponse feedItem() {
        Instant now = Instant.parse("2026-07-31T00:00:00Z");
        return new FeedItemResponse(
                "post-1", "author-1", "author", "Author", "", "content",
                List.of(), "1:1", List.of(), null, List.of(),
                0, 0, 0, false, false, now, now
        );
    }
}
