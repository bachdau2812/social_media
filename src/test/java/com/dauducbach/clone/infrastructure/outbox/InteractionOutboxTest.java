package com.dauducbach.clone.infrastructure.outbox;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class InteractionOutboxTest {
 @Test void resultWaitsForCommitCompletion() {
  var repository = mock(OutboxRepository.class);
  var manager = mock(ReactiveTransactionManager.class);
  var tx = mock(ReactiveTransaction.class);
  var commit = TestPublisher.<Void>create();
  when(manager.getReactiveTransaction(any())).thenReturn(Mono.just(tx));
  when(manager.commit(tx)).thenReturn(commit.mono());
  var outbox = new InteractionOutbox(repository,TransactionalOperator.create(manager));
  StepVerifier.create(outbox.commit(Mono.just("saved"),value -> Mono.empty()))
    .then(() -> verify(manager).commit(tx)).expectNoEvent(java.time.Duration.ofMillis(20))
    .then(commit::complete).expectNext("saved").verifyComplete();
 }
 @Test void approvedCommentPreservesOriginalTimestampAndAddsApprovalTimestamp() {
  var repository = mock(OutboxRepository.class);
  when(repository.append(anyString(),anyString(),anyString(),anyString(),any())).thenReturn(Mono.empty());
  var outbox = new InteractionOutbox(repository,mock(TransactionalOperator.class));
  var comment = new Comment(); comment.setId("comment"); comment.setPostId("post");
  comment.setUserId("actor"); comment.setContent("body"); comment.setModerationStatus("APPROVED");
  var created = Instant.parse("2020-01-01T00:00:00Z"); comment.setTimestamp(created);
  var before = Instant.now();
  StepVerifier.create(outbox.approvedComment(comment)).verifyComplete();
  var json = ArgumentCaptor.forClass(String.class);
  verify(repository).append(eq("COMMENT:comment"),eq("comment_success_event"),eq("actor"),json.capture(),eq(created));
  var payload = JsonParser.parseString(json.getValue()).getAsJsonObject();
  assertEquals(created.toString(),payload.get("occurredAt").getAsString());
  assertEquals("body",payload.get("content").getAsString());
  assertFalse(Instant.parse(payload.get("popularityOccurredAt").getAsString()).isBefore(before));
  comment.setModerationStatus("REJECTED");
  StepVerifier.create(outbox.approvedComment(comment)).expectError(IllegalStateException.class).verify();
  verifyNoMoreInteractions(repository);
 }
}
