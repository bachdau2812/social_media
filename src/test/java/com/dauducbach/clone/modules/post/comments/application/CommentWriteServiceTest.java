package com.dauducbach.clone.modules.post.comments.application;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import com.dauducbach.clone.modules.post.dto.request.CommentCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.CommentUpdateRequest;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.repository.CommentRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommentWriteServiceTest {
    @Mock CommentRepository commentRepository;
    @Mock CommentCountCache commentCountCache;
    @Mock CommentCommitter commentCommitter;
    @Mock CommentUploadWaitStore commentUploadWaitStore;
    @Mock UserSsePublisher userSsePublisher;

    @Test
    void createCommentRejectsEmptyContent() {
        CommentWriteService service = newService();
        CommentCreateRequest request = CommentCreateRequest.builder()
                .postId("post-1").userId("user-1").content(" ").build();

        StepVerifier.create(Mono.defer(() -> service.createComment(request)))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.COMMENT_CONTENT_INVALID)
                .verify();
    }

    @Test
    void updateCommentRejectsNonOwner() {
        CommentWriteService service = newService();
        CommentUpdateRequest request = CommentUpdateRequest.builder()
                .commentId("comment-1").userId("user-2").content("new content").build();
        when(commentRepository.findById("comment-1")).thenReturn(Mono.just(comment("comment-1", "owner-1", null)));

        StepVerifier.create(service.updateComment(request))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.COMMENT_FORBIDDEN)
                .verify();
    }

    @Test
    void updateCommentSavesOwnerChange() {
        CommentWriteService service = newService();
        CommentUpdateRequest request = CommentUpdateRequest.builder()
                .commentId("comment-1").userId("user-1").content("new content").build();
        when(commentRepository.findById("comment-1")).thenReturn(Mono.just(comment("comment-1", "user-1", null)));
        when(commentRepository.save(any(Comment.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(service.updateComment(request))
                .expectNextMatches(updated -> "new content".equals(updated.getContent()))
                .verifyComplete();
    }

    @Test
    void createCommentRejectsReplyToThirdLevel() {
        CommentWriteService service = newService();
        Comment thirdLevel = comment("level-3", "user-2", "level-2");
        Comment secondLevel = comment("level-2", "user-3", "root-1");
        CommentCreateRequest request = CommentCreateRequest.builder()
                .postId("post-1").userId("user-1").parentId("level-3").content("fourth level").build();
        when(commentRepository.findById("level-3")).thenReturn(Mono.just(thirdLevel));
        when(commentRepository.findById("level-2")).thenReturn(Mono.just(secondLevel));

        StepVerifier.create(service.createComment(request))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.COMMENT_CREATE_FAILED)
                .verify();
    }

    private CommentWriteService newService() {
        lenient().when(userSsePublisher.sendToUser(any(), any(), any())).thenReturn(Mono.empty());
        return new CommentWriteService(commentRepository, commentCountCache, commentCommitter,
                commentUploadWaitStore, userSsePublisher);
    }

    private Comment comment(String id, String userId, String parentId) {
        return Comment.builder()
                .id(id).postId("post-1").userId(userId).parentId(parentId)
                .content("content").commentType("TEXT")
                .timestamp(Instant.parse("2026-06-08T00:00:00Z")).build();
    }
}
