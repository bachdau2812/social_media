package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.service.CommentService;
import com.dauducbach.clone.modules.post.service.LikeService;
import com.dauducbach.clone.modules.post.service.MediaService;
import com.dauducbach.clone.modules.post.service.PostFeedQueryService;
import com.dauducbach.clone.modules.post.service.PostService;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FeedServiceTest {
    @Mock
    ReactiveRedisTemplate<String, String> redisTemplate;
    @Mock
    ReactiveZSetOperations<String, String> zSetOperations;
    @Mock
    PostService postService;
    @Mock
    PostFeedQueryService postFeedQueryService;
    @Mock
    MediaService mediaService;
    @Mock
    UserDetailsService userDetailsService;
    @Mock
    LikeService likeService;
    @Mock
    CommentService commentService;
    @Mock
    FeedVectorService feedVectorService;

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
                postService,
                postFeedQueryService,
                mediaService,
                userDetailsService,
                likeService,
                commentService,
                feedVectorService
        );
    }
}
