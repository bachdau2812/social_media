package com.dauducbach.clone.modules.chat.service;
import com.dauducbach.clone.modules.chat.constant.*;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.publicapi.ChatEventType;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.repository.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.*;
import reactor.test.StepVerifier;
import java.time.Instant;
import java.util.List;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.*;

class ChatOutboxMutationTest {
    @Test void delayedCreatedEventCannotRestoreRecalledForwardContent()throws Exception{
        var repository=mock(ChatOutboxRepository.class);var kafka=mock(ChatEventPublisher.class);
        var recipients=mock(MessageReactionRepository.class);var messages=mock(ChatMessageRepository.class);
        var json=new ObjectMapper().findAndRegisterModules();var mapper=new ChatResponseMapper();
        var message=ChatMessage.builder().id("m").conversationId("c").messageSeq(5).senderId("me").messageType(MessageType.TEXT).content("secret").forwarded(true).createdAt(Instant.EPOCH).build();
        var event=ChatEvent.messageCreated(mapper.toChatMessageResponse(message),List.of("me","removed"));
        var lease=new ChatOutboxRepository.LeasedEvent("e","token",json.writeValueAsString(event),1);
        message.setDeletedAt(Instant.now());
        when(repository.candidates(any())).thenReturn(Flux.just("e"));when(repository.lease(eq("e"),any(),any())).thenReturn(Mono.just(lease));
        when(repository.complete(lease)).thenReturn(Mono.empty());when(repository.retry(eq(lease),any())).thenReturn(Mono.empty());
        when(recipients.eligibleRecipients("c",5)).thenReturn(Flux.just("me"));when(messages.findById("m")).thenReturn(Mono.just(message));when(kafka.publish(any())).thenReturn(Mono.empty());
        var publisher=new ChatOutboxPublisher(repository,kafka,recipients,json,messages,mapper);
        StepVerifier.create(publisher.drain()).verifyComplete();
        var captured=org.mockito.ArgumentCaptor.forClass(ChatEvent.class);verify(kafka).publish(captured.capture());
        assertThat(captured.getValue().message().deleted()).isTrue();assertThat(captured.getValue().message().content()).isNull();
        assertThat(captured.getValue().type()).isEqualTo(ChatEventType.MESSAGE_DELETED);
        assertThat(captured.getValue().recipientIds()).containsExactly("me");
    }
    @Test void liveDelayedCreationKeepsItsCoherentOriginalReactionRevision()throws Exception {
        var repository=mock(ChatOutboxRepository.class);var kafka=mock(ChatEventPublisher.class);
        var recipients=mock(MessageReactionRepository.class);var messages=mock(ChatMessageRepository.class);
        var json=new ObjectMapper().findAndRegisterModules();var mapper=new ChatResponseMapper();
        var message=ChatMessage.builder().id("m").conversationId("c").messageSeq(5).senderId("me").messageType(MessageType.TEXT)
            .content("body").forwarded(true).createdAt(Instant.EPOCH).build();
        var event=ChatEvent.messageCreated(mapper.toChatMessageResponse(message),List.of("peer"));
        var lease=new ChatOutboxRepository.LeasedEvent("e","token",json.writeValueAsString(event),1);
        message.setReactionVersion(7);
        when(repository.candidates(any())).thenReturn(Flux.just("e"));when(repository.lease(eq("e"),any(),any())).thenReturn(Mono.just(lease));
        when(repository.complete(lease)).thenReturn(Mono.empty());when(repository.retry(eq(lease),any())).thenReturn(Mono.empty());
        when(recipients.eligibleRecipients("c",5)).thenReturn(Flux.just("peer"));when(messages.findById("m")).thenReturn(Mono.just(message));when(kafka.publish(any())).thenReturn(Mono.empty());
        StepVerifier.create(new ChatOutboxPublisher(repository,kafka,recipients,json,messages,mapper).drain()).verifyComplete();
        var captured=org.mockito.ArgumentCaptor.forClass(ChatEvent.class);verify(kafka).publish(captured.capture());
        assertThat(captured.getValue().message().reactionVersion()).isZero();
        assertThat(captured.getValue().message().likeCount()).isZero();
        assertThat(captured.getValue().type()).isEqualTo(ChatEventType.MESSAGE_CREATED);
    }

    @Test void liveCreatedEventHydratesMessagePresentationBeforeRealtimeDelivery()throws Exception {
        var repository=mock(ChatOutboxRepository.class);var kafka=mock(ChatEventPublisher.class);
        var recipients=mock(MessageReactionRepository.class);var messages=mock(ChatMessageRepository.class);
        var reads=mock(ChatReadRepository.class);var json=new ObjectMapper().findAndRegisterModules();var mapper=new ChatResponseMapper();
        var message=ChatMessage.builder().id("m").conversationId("c").messageSeq(5).senderId("me")
            .messageType(MessageType.TEXT).content("body").createdAt(Instant.EPOCH).build();
        var hydrated=ChatMessage.builder().id("m").conversationId("c").messageSeq(5).senderId("me")
            .senderDisplayName("Alice").messageType(MessageType.TEXT).content("body").createdAt(Instant.EPOCH).build();
        var event=ChatEvent.messageCreated(mapper.toChatMessageResponse(message),List.of("peer"));
        var lease=new ChatOutboxRepository.LeasedEvent("e","token",json.writeValueAsString(event),1);
        when(repository.candidates(any())).thenReturn(Flux.just("e"));when(repository.lease(eq("e"),any(),any())).thenReturn(Mono.just(lease));
        when(repository.complete(lease)).thenReturn(Mono.empty());when(repository.retry(eq(lease),any())).thenReturn(Mono.empty());
        when(recipients.eligibleRecipients("c",5)).thenReturn(Flux.just("peer"));when(messages.findById("m")).thenReturn(Mono.just(message));
        when(reads.findAfterSequence("c",1,4,1)).thenReturn(Flux.just(hydrated));when(kafka.publish(any())).thenReturn(Mono.empty());

        StepVerifier.create(new ChatOutboxPublisher(repository,kafka,recipients,json,messages,reads,mapper).drain()).verifyComplete();
        var captured=org.mockito.ArgumentCaptor.forClass(ChatEvent.class);verify(kafka).publish(captured.capture());
        assertThat(captured.getValue().message().senderDisplayName()).isEqualTo("Alice");
        assertThat(captured.getValue().message().content()).isEqualTo("body");
    }
}
