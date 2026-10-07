package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.ReactionState;
import com.dauducbach.clone.modules.chat.repository.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.time.Instant;
import java.util.List;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ChatReactionOutboxPublisherTest {
    ChatReactionOutboxRepository repository = mock(ChatReactionOutboxRepository.class);
    ChatEventPublisher kafka = mock(ChatEventPublisher.class);
    MessageReactionRepository reactions = mock(MessageReactionRepository.class);
    ObjectMapper mapper = new ObjectMapper();
    ChatReactionOutboxPublisher publisher;
    ChatReactionOutboxRepository.LeasedEvent lease;
    @BeforeEach void setup() throws Exception {
        publisher = new ChatReactionOutboxPublisher(repository, kafka, reactions, mapper);
        var event = ChatEvent.reactionChanged("c", new ReactionState("m", 5, 1, "me", null, 0, List.of()),
                List.of("me", "removed"));
        lease = new ChatReactionOutboxRepository.LeasedEvent("e", "token", mapper.writeValueAsString(event), 1);
        when(repository.candidates(any())).thenReturn(Flux.just("e"));
        when(repository.lease(eq("e"), anyString(), any())).thenReturn(Mono.just(lease));
        when(repository.complete(lease)).thenReturn(Mono.empty());
        when(repository.retry(eq(lease), any())).thenReturn(Mono.empty());
        when(reactions.eligibleRecipients("c", 5)).thenReturn(Flux.just("me"));
        when(kafka.publish(any())).thenReturn(Mono.empty());
    }
    @Test void successfulPublishDeletesOnlyAfterKafkaAndRechecksEligibility() {
        StepVerifier.create(publisher.drain()).verifyComplete();
        var captured = org.mockito.ArgumentCaptor.forClass(ChatEvent.class);
        verify(kafka).publish(captured.capture());
        org.assertj.core.api.Assertions.assertThat(captured.getValue().recipientIds()).containsExactly("me");
        var order = inOrder(kafka, repository);
        order.verify(kafka).publish(any());
        order.verify(repository).complete(lease);
        verify(repository, never()).retry(any(), any());
    }
    @Test void kafkaFailureSchedulesRetryAndKeepsEvent() {
        when(kafka.publish(any())).thenReturn(Mono.error(new IllegalStateException("offline")));
        StepVerifier.create(publisher.drain()).verifyComplete();
        verify(repository).retry(eq(lease), any(Instant.class));
        verify(repository, never()).complete(any());
    }
    @Test void lostLeaseDoesNotPublish() {
        when(repository.lease(eq("e"), anyString(), any())).thenReturn(Mono.empty());
        StepVerifier.create(publisher.drain()).verifyComplete();
        verifyNoInteractions(kafka);
    }
}
