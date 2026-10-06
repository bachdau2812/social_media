package com.dauducbach.clone.infrastructure.outbox;

import com.dauducbach.clone.modules.post.entity.Comment;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.util.function.Function;

@Service
@RequiredArgsConstructor
public class InteractionOutbox {
    private final OutboxRepository repository;
    private final TransactionalOperator transactions;

    public <T> Mono<T> commit(Mono<T> mutation, Function<T, Mono<Void>> append) {
        // single emits only after the transactional Flux completes and its commit has finished.
        return transactions.execute(status -> mutation.flatMap(saved -> append.apply(saved).thenReturn(saved))).single();
    }

    public Mono<Void> append(String eventId, String topic, String userId, JsonObject payload, Instant occurredAt) {
        payload.addProperty("eventId", eventId);
        payload.addProperty("occurredAt", occurredAt.toString());
        return repository.append(eventId, topic, userId, payload.toString(), occurredAt);
    }

    public Mono<Void> append(String eventId, String topic, String aggregateId, String recordKey, JsonObject payload, Instant occurredAt) {
        return repository.append(eventId, topic, aggregateId, recordKey, payload.toString(), occurredAt);
    }

    public Mono<Void> approvedComment(Comment comment) {
        if (!"APPROVED".equals(comment.getModerationStatus())) {
            return Mono.error(new IllegalStateException("Only approved comments may create an interaction"));
        }
        JsonObject payload = new JsonObject();
        payload.addProperty("popularityOccurredAt", Instant.now().toString());
        payload.addProperty("commentId", comment.getId());
        payload.addProperty("userId", comment.getUserId());
        payload.addProperty("postId", comment.getPostId());
        payload.addProperty("content", comment.getContent());
        payload.addProperty("mediaUrl", comment.getMediaUrl());
        payload.addProperty("parentId", comment.getParentId());
        return append("COMMENT:" + comment.getId(), "comment_success_event", comment.getUserId(), payload, comment.getTimestamp());
    }
}
