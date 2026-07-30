package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.service.CommentService;
import com.dauducbach.clone.modules.post.service.LikeService;
import com.dauducbach.clone.modules.post.service.PostDetailQueryService;
import com.dauducbach.clone.modules.post.service.PostFeedQueryService;
import com.dauducbach.clone.modules.post.service.RepostService;
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
    void rejectedArchivedOrDeletedPostStopsBeforeCacheHydration() {
        when(postFeedQueryService.getApprovedPostById("post-1")).thenReturn(Mono.empty());

        StepVerifier.create(hydrator.hydrate("viewer-1", "post-1", null))
                .verifyComplete();

        verifyNoInteractions(redisTemplate);
    }
}