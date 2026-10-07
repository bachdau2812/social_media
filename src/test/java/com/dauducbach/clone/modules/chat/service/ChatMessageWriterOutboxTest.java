package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.repository.ChatOutboxRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChatMessageWriterOutboxTest {
    @Mock R2dbcEntityTemplate template;
    @Mock ConversationRepository conversationRepository;
    @Mock ChatOutboxRepository outbox;
    @Mock ReactiveInsertOperation.ReactiveInsert<ChatMessage> insertOperation;
    @Spy ChatResponseMapper mapper = new ChatResponseMapper();
    @InjectMocks ChatMessageWriter writer;

    @Test
    void outboxFailureFailsTheMessageWriteSoItsTransactionCanRollBack() {
        ChatMessage message = ChatMessage.builder()
                .id("message-1")
                .conversationId("conversation-1")
                .messageSeq(1L)
                .clientMessageId("client-1")
                .senderId("actor-1")
                .messageType(MessageType.TEXT)
                .content("hello")
                .createdAt(Instant.parse("2026-08-01T00:00:00Z"))
                .build();
        Conversation conversation = Conversation.builder()
                .id("conversation-1")
                .build();
        when(template.insert(ChatMessage.class)).thenReturn(insertOperation);
        when(insertOperation.using(message)).thenReturn(Mono.just(message));
        when(conversationRepository.updateMessageSummary(any(), any(Long.class), any(), any()))
                .thenReturn(Mono.just(1));
        when(outbox.append(any())).thenReturn(Mono.error(new IllegalStateException("outbox unavailable")));

        StepVerifier.create(writer.write(message, conversation, Mono.empty(), java.util.List.of("recipient-1")))
                .expectErrorMessage("outbox unavailable")
                .verify();
    }

    @Test
    void appendsCreatedEventAfterTheMessageAndSummaryAreWritten() {
        ChatMessage message = ChatMessage.builder()
                .id("message-1")
                .conversationId("conversation-1")
                .messageSeq(1L)
                .clientMessageId("client-1")
                .senderId("actor-1")
                .messageType(MessageType.TEXT)
                .content("hello")
                .createdAt(Instant.parse("2026-08-01T00:00:00Z"))
                .build();
        Conversation conversation = Conversation.builder().id("conversation-1").build();
        when(template.insert(ChatMessage.class)).thenReturn(insertOperation);
        when(insertOperation.using(message)).thenReturn(Mono.just(message));
        when(conversationRepository.updateMessageSummary(any(), any(Long.class), any(), any()))
                .thenReturn(Mono.just(1));
        when(outbox.append(any())).thenReturn(Mono.empty());

        StepVerifier.create(writer.write(message, conversation, Mono.empty(), java.util.List.of("recipient-1")))
                .expectNext(message)
                .verifyComplete();

        var event = org.mockito.ArgumentCaptor.forClass(ChatEvent.class);
        verify(outbox).append(event.capture());
        assertThat(event.getValue().type()).isEqualTo(com.dauducbach.clone.modules.chat.publicapi.ChatEventType.MESSAGE_CREATED);
        assertThat(event.getValue().message().id()).isEqualTo("message-1");
        assertThat(event.getValue().recipientIds()).containsExactly("recipient-1");
        var order = inOrder(insertOperation, conversationRepository, outbox);
        order.verify(insertOperation).using(message);
        order.verify(conversationRepository).updateMessageSummary(eq("conversation-1"), eq(1L), eq("message-1"), any());
        order.verify(outbox).append(any());
    }
}
