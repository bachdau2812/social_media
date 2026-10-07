package com.dauducbach.clone.modules.post.comments.query;

import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.publicapi.CommentQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommentQueryServiceTest {
    @Mock CommentReadService commentReadService;

    @Test
    void returnsAReadSnapshotWithoutExposingTheCommentEntity() {
        when(commentReadService.getCommentById("comment-1")).thenReturn(Mono.just(Comment.builder()
                .id("comment-1").postId("post-1").userId("user-1").parentId("parent-1")
                .content("hello").timestamp(Instant.parse("2026-01-01T00:00:00Z")).build()));
        var query = new CommentQueryService(commentReadService);

        StepVerifier.create(query.findById("comment-1"))
                .expectNext(new CommentQuery.CommentSnapshot("comment-1", "post-1", "user-1", "parent-1", "hello"))
                .verifyComplete();
    }
}
