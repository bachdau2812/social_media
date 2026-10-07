package com.dauducbach.clone.modules.post.comments.infrastructure.persistence;

import com.dauducbach.clone.infrastructure.outbox.InteractionOutbox;
import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransactionalCommentCommitterTest {
    @Mock R2dbcEntityTemplate entityTemplate;
    @Mock InteractionOutbox interactionOutbox;
    @Mock ReactiveInsertOperation.ReactiveInsert<Comment> insertSpec;

    @Test
    void approvedCommentIsInsertedWithItsOutboxEventInOneCommit() {
        Comment comment = comment("APPROVED");
        stubTransactionalCommit(comment);
        when(interactionOutbox.approvedComment(comment)).thenReturn(Mono.empty());

        StepVerifier.create(new TransactionalCommentCommitter(entityTemplate, interactionOutbox).saveApproved(comment))
                .expectNext(comment)
                .verifyComplete();

        verify(interactionOutbox).approvedComment(comment);
    }

    @Test
    void pendingMediaCommentAppendsTheScanEventWithOriginalPayload() {
        Comment comment = comment("PENDING");
        MediaUploadRequest media = MediaUploadRequest.builder()
                .secureUrl("https://cdn.example/media")
                .publicId("asset-1")
                .resourceType("image")
                .build();
        stubTransactionalCommit(comment);
        when(interactionOutbox.append(eq("COMMENT_SCAN:comment-1"), eq("check_comment_media_event"),
                eq("user-1"), any(JsonObject.class), eq(comment.getTimestamp())))
                .thenReturn(Mono.empty());

        StepVerifier.create(new TransactionalCommentCommitter(entityTemplate, interactionOutbox)
                        .savePendingMedia(comment, List.of(media)))
                .expectNext(comment)
                .verifyComplete();

        ArgumentCaptor<JsonObject> payload = ArgumentCaptor.forClass(JsonObject.class);
        verify(interactionOutbox).append(eq("COMMENT_SCAN:comment-1"), eq("check_comment_media_event"),
                eq("user-1"), payload.capture(), eq(comment.getTimestamp()));
        assertEquals("comment-1", payload.getValue().get("commentId").getAsString());
        assertEquals("post-1", payload.getValue().get("postId").getAsString());
        assertEquals("user-1", payload.getValue().get("userId").getAsString());
        assertEquals("asset-1", payload.getValue().getAsJsonArray("media")
                .get(0).getAsJsonObject().get("publicId").getAsString());
    }

    @SuppressWarnings("unchecked")
    private void stubTransactionalCommit(Comment comment) {
        when(entityTemplate.insert(Comment.class)).thenReturn(insertSpec);
        when(insertSpec.using(comment)).thenReturn(Mono.just(comment));
        when(interactionOutbox.commit(any(), any())).thenAnswer(invocation -> {
            Mono<Comment> insert = invocation.getArgument(0);
            Function<Comment, Mono<Void>> append = invocation.getArgument(1);
            return insert.flatMap(saved -> append.apply(saved).thenReturn(saved));
        });
    }

    private Comment comment(String moderationStatus) {
        return Comment.builder().id("comment-1").postId("post-1").userId("user-1")
                .content("hello").moderationStatus(moderationStatus)
                .timestamp(Instant.parse("2026-01-01T00:00:00Z")).build();
    }
}
