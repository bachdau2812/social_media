package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.modules.chat.constant.*;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.publicapi.ChatEventType;
import com.dauducbach.clone.modules.chat.dto.response.*;
import com.dauducbach.clone.modules.chat.entity.*;
import com.dauducbach.clone.modules.chat.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.time.Instant;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MessageReactionServiceTest {
    MessageReactionRepository reactions = mock(MessageReactionRepository.class);
    ConversationRepository conversations = mock(ConversationRepository.class);
    ConversationMemberRepository members = mock(ConversationMemberRepository.class);
    ChatAccessService access = mock(ChatAccessService.class);
    ChatReactionOutboxRepository outbox = mock(ChatReactionOutboxRepository.class);
    TransactionalOperator tx = mock(TransactionalOperator.class);
    MessageReactionService service;
    ConversationMember member;

    @BeforeEach
    void setup() {
        service = new MessageReactionService(reactions, conversations, members, access, outbox, tx);
        when(tx.transactional(any(Mono.class))).thenAnswer(i -> i.getArgument(0));
        when(conversations.findByIdForUpdate("c")).thenReturn(Mono.just(Conversation.builder().id("c").build()));
        member = ConversationMember.builder().memberStatus(MemberStatus.ACTIVE).joinedSeq(1).build();
        when(members.findMembershipForUpdate("c", "me")).thenReturn(Mono.just(member));
        when(access.requireActiveMember("c", "me")).thenReturn(Mono.just(member));
        row(MessageType.TEXT, null, 5);
        when(reactions.findMine("m", "me")).thenReturn(Mono.empty());
        when(reactions.set(anyString(), anyString(), any(), any())).thenReturn(Mono.empty());
        when(reactions.remove("m", "me")).thenReturn(Mono.empty());
        when(reactions.incrementVersion("m")).thenReturn(Mono.empty());
        when(reactions.eligibleRecipients("c", 5)).thenReturn(Flux.just("me", "other"));
        when(outbox.append(any())).thenReturn(Mono.empty());
        snapshot(null, 0);
    }

    void row(MessageType type, Instant deleted, long seq) {
        when(reactions.lockMessage("c", "m")).thenReturn(Mono.just(
                new MessageReactionRepository.MessageRow("m", seq, type, deleted, 0)));
    }

    void snapshot(ReactionType type, long version) {
        List<ReactionCount> counts = type == null ? List.of() : List.of(new ReactionCount(type, 1));
        when(reactions.snapshots(eq("c"), eq(List.of("m")), eq("me"), anyLong()))
                .thenReturn(Mono.just(List.of(new ReactionSnapshot("m", 5, version, type,
                        type == ReactionType.HEART, type == ReactionType.HEART ? 1 : 0, counts))));
    }

    @ParameterizedTest @EnumSource(ReactionType.class)
    void acceptsAllSixCodesAndPublishesNeutralState(ReactionType type) {
        snapshot(type, 1);
        StepVerifier.create(service.set("me", "c", "m", type))
                .assertNext(state -> {
                    assertThat(state.reaction()).isEqualTo(type);
                    assertThat(state.reactionVersion()).isEqualTo(1);
                    assertThat(state.likeCount()).isEqualTo(type == ReactionType.HEART ? 1 : 0);
                }).verifyComplete();
        var event = org.mockito.ArgumentCaptor.forClass(ChatEvent.class);
        verify(outbox).append(event.capture());
        assertThat(event.getValue().type()).isEqualTo(ChatEventType.MESSAGE_REACTION_CHANGED);
        assertThat(event.getValue().recipientIds()).containsExactly("me", "other");
        assertThat(event.getValue().message()).isNull();
        assertThat(event.getValue().readSeq()).isNull();
        verify(reactions).incrementVersion("m");
    }

    @Test void sameCodePutDoesNotChangeRevisionOrPublish() {
        when(reactions.findMine("m", "me")).thenReturn(Mono.just(ReactionType.HEART));
        snapshot(ReactionType.HEART, 7);
        StepVerifier.create(service.set("me", "c", "m", ReactionType.HEART))
                .expectNextMatches(s -> s.reactionVersion() == 7).verifyComplete();
        verify(reactions, never()).incrementVersion(any());
        verifyNoInteractions(outbox);
    }

    @Test void changeReplacesExistingReaction() {
        when(reactions.findMine("m", "me")).thenReturn(Mono.just(ReactionType.HEART));
        snapshot(ReactionType.ANGRY, 2);
        StepVerifier.create(service.set("me", "c", "m", ReactionType.ANGRY))
                .expectNextMatches(s -> s.reaction() == ReactionType.ANGRY && s.likeCount() == 0)
                .verifyComplete();
        verify(reactions).set(eq("m"), eq("me"), eq(ReactionType.ANGRY), any());
    }

    @Test void removalReturnsNullAndPersistsEvent() {
        when(reactions.findMine("m", "me")).thenReturn(Mono.just(ReactionType.HEART));
        snapshot(null, 2);
        StepVerifier.create(service.remove("me", "c", "m"))
                .expectNextMatches(s -> s.reaction() == null && s.likeCount() == 0 && s.reactionVersion() == 2)
                .verifyComplete();
        verify(reactions).remove("m", "me");
        verify(outbox).append(any());
    }

    @Test void absentDeleteIsIdempotent() {
        StepVerifier.create(service.remove("me", "c", "m")).expectNextCount(1).verifyComplete();
        verify(reactions, never()).remove(any(), any());
        verifyNoInteractions(outbox);
    }
    @Test void outboxFailureFailsMutationForTransactionRollback() {
        snapshot(ReactionType.HEART, 1);
        when(outbox.append(any())).thenReturn(Mono.error(new IllegalStateException("DB unavailable")));
        StepVerifier.create(service.set("me", "c", "m", ReactionType.HEART))
                .expectError(IllegalStateException.class).verify();
    }
    @Test void paginationClampsLimitAndRejectsLongCursor() {
        when(reactions.list("m", null, "", 101)).thenReturn(Flux.empty());
        StepVerifier.create(service.list("me", "c", "m", null, null, 10000))
                .expectNextMatches(page -> page.items().isEmpty() && !page.hasMore()).verifyComplete();
        verify(reactions).list("m", null, "", 101);
        StepVerifier.create(service.list("me", "c", "m", null, "x".repeat(65), 30))
                .expectError(AppException.class).verify();
    }

    @Test void rejectsInactiveMembershipInsideLock() {
        member.setMemberStatus(MemberStatus.LEFT);
        rejects();
    }

    @Test void rejectsInvisibleMessage() {
        member.setLastDeletedMessageSeq(5L);
        rejects();
    }

    @Test void rejectsBeforeJoin() {
        member.setJoinedSeq(6L);
        rejects();
    }

    @Test void rejectsDeletedMessage() { row(MessageType.TEXT, Instant.now(), 5); rejects(); }
    @Test void rejectsSystemMessage() { row(MessageType.SYSTEM, null, 5); rejects(); }
    @Test void rejectsWrongConversation() {
        when(reactions.lockMessage("c", "m")).thenReturn(Mono.empty());
        rejects();
    }

    @Test void rejectsDissolvedMutation() {
        when(conversations.findByIdForUpdate("c")).thenReturn(Mono.just(
                Conversation.builder().id("c").isDissolved(true).build()));
        rejects();
    }

    void rejects() {
        StepVerifier.create(service.set("me", "c", "m", ReactionType.HEART))
                .expectError(AppException.class).verify();
        verifyNoInteractions(outbox);
        verify(reactions, never()).incrementVersion(any());
    }

    @Test void rejectsOversizedBatchBeforeQuery() {
        StepVerifier.create(service.getSnapshots("me", "c", java.util.Collections.nCopies(101, "m")))
                .expectError(AppException.class).verify();
        verifyNoInteractions(access);
    }

    @Test void batchRejectsMissingOrInvisibleIds() {
        when(reactions.snapshots("c", List.of("missing"), "me", 1)).thenReturn(Mono.just(List.of()));
        StepVerifier.create(service.getSnapshots("me", "c", List.of("missing")))
                .expectError(AppException.class).verify();
    }

    @Test void filteredPaginationIsBoundedAndStable() {
        var rows = List.of(new MessageReactorResponse("a", "A", null, ReactionType.HEART, Instant.EPOCH),
                new MessageReactorResponse("b", "B", null, ReactionType.HEART, Instant.EPOCH));
        when(reactions.list("m", ReactionType.HEART, "", 2)).thenReturn(Flux.fromIterable(rows));
        StepVerifier.create(service.list("me", "c", "m", ReactionType.HEART, null, 1))
                .assertNext(page -> {
                    assertThat(page.items()).hasSize(1);
                    assertThat(page.nextCursor()).isEqualTo("a");
                    assertThat(page.hasMore()).isTrue();
                }).verifyComplete();
    }
}
