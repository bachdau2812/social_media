package com.dauducbach.clone.modules.post.service.comment;

import com.dauducbach.clone.modules.post.service.post.PostSseService;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.dto.request.CommentCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.CommentUpdateRequest;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.repositoty.CommentRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommentServiceTest {
    @Mock
    CommentRepository commentRepository;
    @Mock
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    @Mock
    ReactiveValueOperations<String, String> valueOperations;
    @Mock
    KafkaSender<String, String> kafkaSender;
    @Mock
    PostSseService postSseService;
    @Mock
    R2dbcEntityTemplate r2dbcEntityTemplate;
    @Mock
    ReactiveInsertOperation.ReactiveInsert<Comment> commentInsertSpec;
    @Mock
    MediaCompatibilityFacade cloudinaryMediaService;

    @Test
    void createCommentRejectsEmptyContent() {
        CommentService service = newService();
        CommentCreateRequest request = CommentCreateRequest.builder()
                .postId("post-1")
                .userId("user-1")
                .content(" ")
                .build();

        StepVerifier.create(Mono.defer(() -> service.createComment(request)))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.COMMENT_CONTENT_INVALID)
                .verify();
    }

    @Test
    void updateCommentRejectsNonOwner() {
        CommentService service = newService();
        Comment existing = comment("comment-1", "post-1", "owner-1", null);
        CommentUpdateRequest request = CommentUpdateRequest.builder()
                .commentId("comment-1")
                .userId("user-2")
                .content("new content")
                .build();

        when(commentRepository.findById("comment-1")).thenReturn(Mono.just(existing));

        StepVerifier.create(service.updateComment(request))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.COMMENT_FORBIDDEN)
                .verify();
    }

    @Test
    void updateCommentSavesOwnerChange() {
        CommentService service = newService();
        Comment existing = comment("comment-1", "post-1", "user-1", null);
        CommentUpdateRequest request = CommentUpdateRequest.builder()
                .commentId("comment-1")
                .userId("user-1")
                .content("new content")
                .build();

        when(commentRepository.findById("comment-1")).thenReturn(Mono.just(existing));
        when(commentRepository.save(any(Comment.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(service.updateComment(request))
                .expectNextMatches(comment -> comment.getContent().equals("new content"))
                .verifyComplete();
    }

    @Test
    void getRootCommentsUsesValidatedPagination() {
        CommentService service = newService();
        Comment first = comment("comment-1", "post-1", "user-1", null);

        when(commentRepository.findRootByPostId("post-1", 10, 0)).thenReturn(Flux.just(first));
        when(commentRepository.countByParentId("comment-1")).thenReturn(Mono.just(2L));

        StepVerifier.create(service.getRootComments("post-1", -1, -1))
                .expectNextMatches(comment -> comment.getId().equals("comment-1") && comment.getReplyCount() == 2)
                .verifyComplete();
    }

    @Test
    void createCommentRejectsReplyToThirdLevel() {
        CommentService service = newService();
        Comment thirdLevel = comment("level-3", "post-1", "user-2", "level-2");
        Comment secondLevel = comment("level-2", "post-1", "user-3", "root-1");
        CommentCreateRequest request = CommentCreateRequest.builder()
                .postId("post-1")
                .userId("user-1")
                .parentId("level-3")
                .content("fourth level")
                .build();

        when(commentRepository.findById("level-3")).thenReturn(Mono.just(thirdLevel));
        when(commentRepository.findById("level-2")).thenReturn(Mono.just(secondLevel));

        StepVerifier.create(service.createComment(request))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.COMMENT_CREATE_FAILED)
                .verify();
    }

    @Test
    void getRootCommentsPageReturnsTotalAndReplyCounts() {
        CommentService service = newService();
        Comment first = comment("comment-1", "post-1", "user-1", null);

        when(commentRepository.countRootByPostId("post-1")).thenReturn(Mono.just(12L));
        when(commentRepository.findRootByPostId("post-1", 10, 0)).thenReturn(Flux.just(first));
        when(commentRepository.countByParentId("comment-1")).thenReturn(Mono.just(2L));

        StepVerifier.create(service.getRootCommentsPage("post-1", 0, 10))
                .expectNextMatches(response -> response.totalElements() == 12
                        && response.totalPages() == 2
                        && response.content().get(0).getReplyCount() == 2)
                .verifyComplete();
    }
    @Test
    void getCommentedPostsReturnsPageResponse() {
        CommentService service = newService();

        when(commentRepository.countCommentedPostsByUserId("user-1")).thenReturn(Mono.just(3L));
        when(commentRepository.findCommentedPostIdsByUserId("user-1", 2, 0))
                .thenReturn(Flux.just("post-3", "post-2"));

        StepVerifier.create(service.getCommentedPostIdsByUserId("user-1", 0, 2))
                .expectNextMatches(response -> response.content().size() == 2
                        && response.totalElements() == 3
                        && response.totalPages() == 2)
                .verifyComplete();
    }

    @Test
    void getDistinctCommenterUserIdsByPostIdReadsRepository() {
        CommentService service = newService();

        when(commentRepository.findDistinctUserIdsByPostId("post-1"))
                .thenReturn(Flux.just("user-1", "user-2"));

        StepVerifier.create(service.getDistinctCommenterUserIdsByPostId("post-1"))
                .expectNext("user-1", "user-2")
                .verifyComplete();
    }

    @Test
    void getCommentsByUserReturnsPageResponse() {
        CommentService service = newService();
        Comment first = comment("comment-1", "post-1", "user-1", null);

        when(commentRepository.countByUserId("user-1")).thenReturn(Mono.just(1L));
        when(commentRepository.findByUserId("user-1", 10, 0)).thenReturn(Flux.just(first));

        StepVerifier.create(service.getCommentsByUserId("user-1", 0, 10))
                .expectNextMatches(response -> response.content().contains(first)
                        && response.totalElements() == 1)
                .verifyComplete();
    }

    @Test
    void countCommentsByPostReturnsRepositoryCount() {
        CommentService service = newService();

        when(reactiveRedisStringTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("post_comment_count:post-1")).thenReturn(Mono.empty(), Mono.empty());
        when(valueOperations.setIfAbsent(eq("post_comment_count_lock:post-1"), anyString(), any(Duration.class)))
                .thenReturn(Mono.just(true));
        when(commentRepository.countByPostId("post-1")).thenReturn(Mono.just(5L));
        when(valueOperations.set("post_comment_count:post-1", "5", Duration.ofHours(24))).thenReturn(Mono.just(true));
        when(valueOperations.get("post_comment_count_lock:post-1")).thenReturn(Mono.just("released-by-expiry"));

        StepVerifier.create(service.countCommentsByPostId("post-1"))
                .expectNext(5L)
                .verifyComplete();
    }

    @Test
    void countCommentsByPostReadsCacheFirst() {
        CommentService service = newService();

        when(reactiveRedisStringTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("post_comment_count:post-1")).thenReturn(Mono.just("8"));

        StepVerifier.create(service.countCommentsByPostId("post-1"))
                .expectNext(8L)
                .verifyComplete();

        verify(commentRepository, never()).countByPostId("post-1");
    }

    @Test
    void countRepliesByParentReturnsRepositoryCount() {
        CommentService service = newService();

        when(commentRepository.countByParentId("comment-1")).thenReturn(Mono.just(2L));

        StepVerifier.create(service.countRepliesByParentId("comment-1"))
                .expectNext(2L)
                .verifyComplete();
    }

    private CommentService newService() {
        org.mockito.Mockito.lenient().when(reactiveRedisStringTemplate.opsForValue()).thenReturn(valueOperations);
        org.mockito.Mockito.lenient().when(valueOperations.setIfAbsent(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        org.mockito.Mockito.lenient().when(valueOperations.get(anyString())).thenReturn(Mono.empty());
        org.mockito.Mockito.lenient().when(valueOperations.set(anyString(), anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        org.mockito.Mockito.lenient().when(valueOperations.increment(anyString(), anyLong())).thenReturn(Mono.just(1L));
        org.mockito.Mockito.lenient().when(reactiveRedisStringTemplate.expire(anyString(), any(Duration.class))).thenReturn(Mono.just(true));
        org.mockito.Mockito.lenient().when(postSseService.sendToUser(anyString(), anyString(), anyString())).thenReturn(Mono.empty());
        org.mockito.Mockito.lenient().when(r2dbcEntityTemplate.insert(Comment.class)).thenReturn(commentInsertSpec);
        org.mockito.Mockito.lenient().when(commentInsertSpec.using(any(Comment.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        return new CommentService(commentRepository, reactiveRedisStringTemplate, kafkaSender, postSseService, r2dbcEntityTemplate, cloudinaryMediaService);
    }

    private Comment comment(String id, String postId, String userId, String parentId) {
        return Comment.builder()
                .id(id)
                .postId(postId)
                .userId(userId)
                .parentId(parentId)
                .content("content")
                .commentType("TEXT")
                .timestamp(Instant.parse("2026-06-08T00:00:00Z"))
                .build();
    }
}
