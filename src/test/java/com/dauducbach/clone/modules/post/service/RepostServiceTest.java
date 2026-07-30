package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.dto.response.RepostToggleResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostRepost;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repositoty.PostRepostRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RepostServiceTest {
    @Mock
    PostRepostRepository repostRepository;
    @Mock
    PostDetailsRepository postDetailsRepository;
    @Mock
    R2dbcEntityTemplate r2dbcEntityTemplate;
    @Mock
    ReactiveInsertOperation.ReactiveInsert<PostRepost> insertSpec;
    @Mock
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    @Mock
    ReactiveValueOperations<String, String> valueOperations;

    @Test
    void repostCreatesRelationshipForAnotherUsersPost() {
        RepostService service = newService();

        when(postDetailsRepository.findById("post-1")).thenReturn(Mono.just(post("post-1", "owner-1")));
        when(repostRepository.findByActorIdAndPostId("user-1", "post-1")).thenReturn(Mono.empty());
        when(r2dbcEntityTemplate.insert(PostRepost.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(PostRepost.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(repostRepository.countByPostId("post-1")).thenReturn(Mono.just(1L));

        StepVerifier.create(service.repost("user-1", "post-1"))
                .expectNextMatches(response -> response.reposted()
                        && response.postId().equals("post-1")
                        && response.repostCount() == 1
                        && response.repostId() != null)
                .verifyComplete();
    }

    @Test
    void repostRejectsOwnPost() {
        RepostService service = newService();

        when(postDetailsRepository.findById("post-1")).thenReturn(Mono.just(post("post-1", "user-1")));

        StepVerifier.create(service.repost("user-1", "post-1"))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.REPOST_OWN_POST_NOT_ALLOWED)
                .verify();

        verify(repostRepository, never()).findByActorIdAndPostId(any(), any());
    }

    @Test
    void unrepostDeletesExistingRelationship() {
        RepostService service = newService();
        PostRepost repost = PostRepost.builder().id("repost-1").actorId("user-1").postId("post-1").postOwnerId("owner-1").build();

        when(repostRepository.findByActorIdAndPostId("user-1", "post-1")).thenReturn(Mono.just(repost));
        when(repostRepository.delete(repost)).thenReturn(Mono.empty());
        when(repostRepository.countByPostId("post-1")).thenReturn(Mono.just(0L));

        StepVerifier.create(service.unrepost("user-1", "post-1"))
                .expectNextMatches(response -> !response.reposted()
                        && response.postId().equals("post-1")
                        && response.repostId().equals("repost-1"))
                .verifyComplete();
    }

    @Test
    void getRepostedPostIdsReturnsPagedIds() {
        RepostService service = newService();

        when(repostRepository.countByActorId("user-1")).thenReturn(Mono.just(3L));
        when(repostRepository.findPostIdsByActorId(eq("user-1"), any(Pageable.class))).thenReturn(Flux.just("post-3", "post-2"));

        StepVerifier.create(service.getRepostedPostIds("user-1", 0, 2))
                .expectNextMatches(response -> response.content().size() == 2
                        && response.totalElements() == 3
                        && response.totalPages() == 2)
                .verifyComplete();
    }

    @Test
    void getReposterActorIdsReturnsNewestActorsForPost() {
        RepostService service = newService();

        when(repostRepository.countByPostId("post-1")).thenReturn(Mono.just(3L));
        when(repostRepository.findActorIdsByPostId(eq("post-1"), any(Pageable.class)))
                .thenReturn(Flux.just("user-3", "user-2"));

        StepVerifier.create(service.getReposterActorIds("post-1", 0, 2))
                .expectNextMatches(response -> response.content().equals(java.util.List.of("user-3", "user-2"))
                        && response.totalElements() == 3)
                .verifyComplete();
    }

    private RepostService newService() {
        org.mockito.Mockito.lenient().when(reactiveRedisStringTemplate.opsForValue()).thenReturn(valueOperations);
        org.mockito.Mockito.lenient().when(valueOperations.get(anyString())).thenReturn(Mono.empty());
        org.mockito.Mockito.lenient().when(valueOperations.setIfAbsent(anyString(), anyString(), any(java.time.Duration.class))).thenReturn(Mono.just(true));
        org.mockito.Mockito.lenient().when(valueOperations.set(anyString(), anyString(), any(java.time.Duration.class))).thenReturn(Mono.just(true));
        org.mockito.Mockito.lenient().when(valueOperations.increment(anyString(), anyLong())).thenReturn(Mono.just(0L));
        org.mockito.Mockito.lenient().when(reactiveRedisStringTemplate.expire(anyString(), any(java.time.Duration.class))).thenReturn(Mono.just(true));
        return new RepostService(repostRepository, postDetailsRepository, r2dbcEntityTemplate, reactiveRedisStringTemplate);
    }

    private PostDetails post(String postId, String userId) {
        return PostDetails.builder().postId(postId).userId(userId).content("content").build();
    }
}