package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.exception.*;
import com.dauducbach.clone.infrastructure.outbox.InteractionOutbox;
import com.dauducbach.clone.modules.post.dto.request.PostInteractionRequest;
import com.dauducbach.clone.modules.post.entity.*;
import com.dauducbach.clone.modules.post.repository.PostInteractionReceiptRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostInteractionServiceTest {
    final PostInteractionReceiptRepository receipts = mock(PostInteractionReceiptRepository.class);
    final PostFeedQueryService posts = mock(PostFeedQueryService.class);
    final InteractionOutbox outbox = mock(InteractionOutbox.class);
    final PostInteractionService service = new PostInteractionService(receipts, posts, outbox, new PostInteractionScorePolicy());
    final String event = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    final String impression = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
    PostInteractionRequest request(boolean click, int seconds) {
        return new PostInteractionRequest("post", click, seconds, event, impression);
    }
    @BeforeEach void enabled() { ReflectionTestUtils.setField(service, "ingestionEnabled", true); }
    @Test void disabledAndInvalidHaveNoPersistenceEffects() {
        ReflectionTestUtils.setField(service, "ingestionEnabled", false);
        StepVerifier.create(service.accept("actor",request(true,60))).expectErrorMatches(e ->
                e instanceof AppException a && a.getErrorCode() == ErrorCode.POST_INTERACTION_UNAVAILABLE).verify();
        ReflectionTestUtils.setField(service, "ingestionEnabled", true);
        StepVerifier.create(service.accept("actor",request(true,3601))).expectError(AppException.class).verify();
        StepVerifier.create(service.accept("actor",new PostInteractionRequest("post",true,1,"1-1-1-1-1",impression)))
                .expectError(AppException.class).verify();
        verifyNoInteractions(receipts, posts, outbox);
    }
    @Test void missingPostNeverAppends() {
        when(receipts.find("actor",event)).thenReturn(Mono.empty());
        when(posts.getApprovedPostById("post")).thenReturn(Mono.empty());
        StepVerifier.create(service.accept("actor",request(true,60))).expectErrorMatches(e ->
                e instanceof AppException a && a.getErrorCode() == ErrorCode.POST_NOT_FOUND).verify();
        verifyNoInteractions(outbox); verify(receipts,never()).insert(any());
    }
    @SuppressWarnings("unchecked")
    @Test void commitsBeforeResponseAndRetriesReturnOriginalOrConflict() {
        AtomicBoolean committed = new AtomicBoolean();
        var saved = new java.util.concurrent.atomic.AtomicReference<PostInteractionReceipt>();
        when(receipts.find("actor",event)).thenAnswer(inv -> Mono.justOrEmpty(saved.get()));
        when(posts.getApprovedPostById("post")).thenReturn(Mono.just(new PostDetails()));
        when(receipts.insert(any())).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(outbox.append(anyString(),anyString(),anyString(),anyString(),any(),any())).thenReturn(Mono.empty());
        when(outbox.commit(any(),any())).thenAnswer(inv -> {
            Mono<PostInteractionReceipt> mutation = inv.getArgument(0);
            Function<PostInteractionReceipt,Mono<Void>> append = inv.getArgument(1);
            return mutation.flatMap(r -> append.apply(r).thenReturn(r)).doOnNext(r -> {
                saved.set(r); committed.set(true);
            });
        });
        StepVerifier.create(service.accept("actor",request(true,61))).assertNext(r -> {
            assertTrue(committed.get()); assertFalse(r.duplicate()); assertEquals(3,r.computedScore());
        }).verifyComplete();
        StepVerifier.create(service.accept("actor",request(true,61))).assertNext(r -> assertTrue(r.duplicate())).verifyComplete();
        StepVerifier.create(service.accept("actor",request(false,61))).expectErrorMatches(e ->
                e instanceof AppException a && a.getErrorCode() == ErrorCode.POST_INTERACTION_BODY_CONFLICT).verify();
        verify(outbox,times(1)).commit(any(),any());
        verify(outbox).append(eq("POST_INTERACTION:actor:"+event),eq("post_interaction"),eq("actor"),eq("post"),any(),any());
    }
    @Test void concurrentDuplicateReturnsWinningReceiptWithoutSecondAppend() {
        var saved = new java.util.concurrent.atomic.AtomicReference<PostInteractionReceipt>();
        when(receipts.find("actor",event)).thenReturn(Mono.empty(), Mono.defer(() -> Mono.just(saved.get())));
        when(posts.getApprovedPostById("post")).thenReturn(Mono.just(new PostDetails()));
        when(receipts.insert(any())).thenAnswer(inv -> {
            saved.set(inv.getArgument(0)); return Mono.error(new DuplicateKeyException("race"));
        });
        when(outbox.commit(any(),any())).thenAnswer(inv -> ((Mono<?>) inv.getArgument(0)));
        StepVerifier.create(service.accept("actor",request(true,1))).assertNext(r -> {
            assertTrue(r.duplicate()); assertEquals(1,r.computedScore());
        }).verifyComplete();
        verify(receipts,times(2)).find("actor",event);
        verify(outbox,never()).append(anyString(),anyString(),anyString(),anyString(),any(),any());
    }
    @Test void uniqueRaceRecoveryRunsAfterFailedTransaction() {
        when(receipts.find("actor",event)).thenReturn(Mono.empty(), Mono.just(
                new PostInteractionReceipt("actor",event,"post",impression,"different",1,Instant.now())));
        when(posts.getApprovedPostById("post")).thenReturn(Mono.just(new PostDetails()));
        when(receipts.insert(any())).thenReturn(Mono.error(new DuplicateKeyException("race")));
        when(outbox.commit(any(),any())).thenReturn(Mono.error(new DuplicateKeyException("race")));
        StepVerifier.create(service.accept("actor",request(true,1))).expectErrorMatches(e ->
                e instanceof AppException a && a.getErrorCode() == ErrorCode.POST_INTERACTION_BODY_CONFLICT).verify();
        verify(receipts,times(2)).find("actor",event);
    }
}
