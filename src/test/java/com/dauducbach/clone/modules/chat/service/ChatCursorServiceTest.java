package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.publicapi.ChatEventType;
import com.dauducbach.clone.modules.chat.repository.ChatOutboxRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

class ChatCursorServiceTest {
    private final ChatAccessService access = mock(ChatAccessService.class);
    private final ConversationMemberRepository members = mock(ConversationMemberRepository.class);
    private final ConversationRepository conversations = mock(ConversationRepository.class);
    private final ChatOutboxRepository outbox = mock(ChatOutboxRepository.class);
    private final TransactionalOperator transaction = mock(TransactionalOperator.class);
    private ChatCursorService service;

    @BeforeEach
    void setUp() {
        service = new ChatCursorService(access, members, conversations, outbox, transaction);
        when(transaction.transactional(any(Mono.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(conversations.findById("conversation-1"))
                .thenReturn(Mono.just(Conversation.builder().id("conversation-1").lastMessageSeq(10L).build()));
        when(access.requireActiveMember("conversation-1", "actor-1"))
                .thenReturn(Mono.just(ConversationMember.builder()
                        .lastDeliveredSeq(3L).lastReadSeq(2L).build()));
        when(members.findActiveUserIds("conversation-1")).thenReturn(Flux.just("actor-1", "peer-1"));
        when(outbox.append(any())).thenReturn(Mono.empty());
    }

    @Test
    void deliveredCursorAndRealtimeIntentAreWrittenTogether() {
        when(members.advanceDeliveredSequence("conversation-1", "actor-1", 7L)).thenReturn(Mono.just(1));

        StepVerifier.create(service.markDelivered("actor-1", "conversation-1", 7L))
                .assertNext(cursor -> {
                    assertThat(cursor.deliveredSeq()).isEqualTo(7L);
                    assertThat(cursor.readSeq()).isEqualTo(2L);
                })
                .verifyComplete();

        var event = org.mockito.ArgumentCaptor.forClass(ChatEvent.class);
        verify(outbox).append(event.capture());
        assertThat(event.getValue().type()).isEqualTo(ChatEventType.CURSOR_UPDATED);
        assertThat(event.getValue().deliveredSeq()).isEqualTo(7L);
        assertThat(event.getValue().recipientIds()).containsExactly("peer-1");
    }

    @Test
    void outboxFailureRollsBackTheCursorUpdate() {
        when(members.advanceDeliveredSequence("conversation-1", "actor-1", 7L)).thenReturn(Mono.just(1));
        when(outbox.append(any())).thenReturn(Mono.error(new IllegalStateException("outbox unavailable")));

        StepVerifier.create(service.markDelivered("actor-1", "conversation-1", 7L))
                .expectError(AppException.class)
                .verify();
    }

    @Test
    void readCursorAlsoAdvancesDeliveredAndNeverRegresses() {
        when(members.advanceDeliveredAndReadSequence("conversation-1", "actor-1", 6L)).thenReturn(Mono.just(1));

        StepVerifier.create(service.markRead("actor-1", "conversation-1", 6L))
                .assertNext(cursor -> {
                    assertThat(cursor.deliveredSeq()).isEqualTo(6L);
                    assertThat(cursor.readSeq()).isEqualTo(6L);
                })
                .verifyComplete();
        verify(members).advanceDeliveredAndReadSequence(eq("conversation-1"), eq("actor-1"), anyLong());
    }
}
