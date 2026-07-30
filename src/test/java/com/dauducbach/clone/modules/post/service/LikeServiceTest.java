package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.post.dto.request.LikeRequest;
import com.dauducbach.clone.modules.post.entity.Like;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repositoty.CommentRepository;
import com.dauducbach.clone.modules.post.repositoty.LikeRepository;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LikeServiceTest {
    @Mock
    LikeRepository likeRepository;
    @Mock
    PostDetailsRepository postDetailsRepository;
    @Mock
    CommentRepository commentRepository;
    @Mock
    KafkaSender<String, String> kafkaSender;
    @Mock
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    @Mock
    ReactiveValueOperations<String, String> valueOperations;
    @Mock
    R2dbcEntityTemplate r2dbcEntityTemplate;
    @Mock
    ReactiveInsertOperation.ReactiveInsert<Like> likeInsertSpec;
    @Mock
    UserAuditService userAuditService;

    @Test
    void likeSavesRecordAndPublishesEvent() {
        LikeService service = newService();
        LikeRequest request = new LikeRequest("post-1", "post");

        when(postDetailsRepository.findById("post-1")).thenReturn(Mono.just(post("post-1", "owner-1")));
        when(likeRepository.findByActorIdAndTargetIdAndTargetType("user-1", "post-1", "POST"))
                .thenReturn(Mono.empty());
        when(r2dbcEntityTemplate.insert(Like.class)).thenReturn(likeInsertSpec);
        when(likeInsertSpec.using(any(Like.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(kafkaSender.send(any(Publisher.class))).thenReturn(Flux.empty());
        mockPostLikeCacheUpdate(1, 1);

        StepVerifier.create(service.like("user-1", request))
                .expectNextMatches(response -> response.liked()
                        && response.targetId().equals("post-1")
                        && response.targetType().equals("POST")
                        && response.likeId() != null)
                .verifyComplete();
    }

    @Test
    void likeRejectsInvalidTargetType() {
        LikeService service = newService();

        StepVerifier.create(Mono.defer(() -> service.like("user-1", new LikeRequest("post-1", "STORY"))))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.INVALID_TARGET_TYPE)
                .verify();
    }

    @Test
    void likeRejectsMissingTarget() {
        LikeService service = newService();
        LikeRequest request = new LikeRequest("post-1", "POST");

        when(likeRepository.findByActorIdAndTargetIdAndTargetType("user-1", "post-1", "POST"))
                .thenReturn(Mono.empty());
        when(postDetailsRepository.findById(anyString())).thenReturn(Mono.empty());

        StepVerifier.create(service.like("user-1", request))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.TARGET_NOT_FOUND)
                .verify();

        verify(likeRepository, never()).save(any());
    }

    @Test
    void likeDeletesExistingLikeWhenAlreadyLiked() {
        LikeService service = newService();
        LikeRequest request = new LikeRequest("post-1", "POST");
        Like like = Like.builder().id("like-1").actorId("user-1").targetId("post-1").targetType("POST").build();

        when(likeRepository.findByActorIdAndTargetIdAndTargetType("user-1", "post-1", "POST"))
                .thenReturn(Mono.just(like));
        when(likeRepository.delete(like)).thenReturn(Mono.empty());
        mockPostLikeCacheUpdate(1, 0);

        StepVerifier.create(service.like("user-1", request))
                .expectNextMatches(response -> !response.liked()
                        && response.targetId().equals("post-1")
                        && response.targetType().equals("POST")
                        && response.likeId().equals("like-1"))
                .verifyComplete();

        verify(likeRepository, never()).save(any());
        verify(kafkaSender, never()).send(any(Publisher.class));
    }

    @Test
    void hasLikedReturnsRepositoryResult() {
        LikeService service = newService();

        when(likeRepository.existsByActorIdAndTargetIdAndTargetType("user-1", "post-1", "POST"))
                .thenReturn(Mono.just(true));

        StepVerifier.create(service.hasLiked("user-1", "post-1", "POST"))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void getLikedTargetsReturnsPageResponse() {
        LikeService service = newService();

        when(likeRepository.countByActorIdAndTargetType("user-1", "POST")).thenReturn(Mono.just(3L));
        when(likeRepository.findTargetIdByActorIdAndTargetType(eq("user-1"), eq("POST"), any(Pageable.class)))
                .thenReturn(Flux.just("post-3", "post-2"));

        StepVerifier.create(service.getLikedTargets("user-1", "POST", 0, 2))
                .expectNextMatches(response -> response.content().size() == 2
                        && response.totalElements() == 3
                        && response.totalPages() == 2)
                .verifyComplete();
    }

    @Test
    void getLikerActorIdsReturnsNewestActorsForTarget() {
        LikeService service = newService();

        when(likeRepository.countByTargetIdAndTargetType("post-1", "POST")).thenReturn(Mono.just(3L));
        when(likeRepository.findActorIdsByTargetIdAndTargetType(eq("post-1"), eq("POST"), any(Pageable.class)))
                .thenReturn(Flux.just("user-3", "user-2"));

        StepVerifier.create(service.getLikerActorIds("post-1", "POST", 0, 2))
                .expectNextMatches(response -> response.content().equals(java.util.List.of("user-3", "user-2"))
                        && response.totalElements() == 3)
                .verifyComplete();
    }

    @Test
    void countLikesForPostReadsCacheFirst() {
        LikeService service = newService();

        when(reactiveRedisStringTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("post_like_count:post-1")).thenReturn(Mono.just("9"));

        StepVerifier.create(service.countLikes("post-1", "POST"))
                .expectNext(9L)
                .verifyComplete();

        verify(likeRepository, never()).countByTargetIdAndTargetType("post-1", "POST");
    }

    @Test
    void countLikesForPostLoadsDbWhenCacheMisses() {
        LikeService service = newService();

        when(reactiveRedisStringTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("post_like_count:post-1")).thenReturn(Mono.empty(), Mono.empty());
        when(valueOperations.setIfAbsent(eq("post_like_count_lock:post-1"), anyString(), any(Duration.class)))
                .thenReturn(Mono.just(true));
        when(likeRepository.countByTargetIdAndTargetType("post-1", "POST")).thenReturn(Mono.just(4L));
        when(valueOperations.set("post_like_count:post-1", "4", Duration.ofHours(24))).thenReturn(Mono.just(true));
        when(valueOperations.get("post_like_count_lock:post-1")).thenReturn(Mono.just("released-by-expiry"));

        StepVerifier.create(service.countLikes("post-1", "POST"))
                .expectNext(4L)
                .verifyComplete();
    }

    @Test
    void countLikesForCommentBypassesCache() {
        LikeService service = newService();

        when(likeRepository.countByTargetIdAndTargetType("comment-1", "COMMENT")).thenReturn(Mono.just(3L));

        StepVerifier.create(service.countLikes("comment-1", "COMMENT"))
                .expectNext(3L)
                .verifyComplete();
    }

    private LikeService newService() {
        return new LikeService(likeRepository, postDetailsRepository, commentRepository, kafkaSender, reactiveRedisStringTemplate, r2dbcEntityTemplate);
    }

    private void mockPostLikeCacheUpdate(long currentCount, long updatedCount) {
        when(reactiveRedisStringTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.setIfAbsent(eq("post_like_count_lock:post-1"), anyString(), any(Duration.class)))
                .thenReturn(Mono.just(true), Mono.just(true));
        when(valueOperations.get("post_like_count:post-1")).thenReturn(Mono.just(String.valueOf(currentCount)));
        lenient().when(likeRepository.countByTargetIdAndTargetType("post-1", "POST")).thenReturn(Mono.just(currentCount));
        when(valueOperations.increment(eq("post_like_count:post-1"), anyLong())).thenReturn(Mono.just(updatedCount));
        when(reactiveRedisStringTemplate.expire("post_like_count:post-1", Duration.ofHours(24))).thenReturn(Mono.just(true));
        when(valueOperations.get("post_like_count_lock:post-1")).thenReturn(Mono.just("released-by-expiry"));
    }

    private PostDetails post(String postId, String userId) {
        return PostDetails.builder()
                .postId(postId)
                .userId(userId)
                .content("content")
                .build();
    }
}
