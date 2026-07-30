package com.dauducbach.clone.modules.chat.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChatRealtimeEventListenerTest {

    @Test
    void kafkaCompletionWaitsForRedisFanout() {
        ChatRealtimeFanoutPublisher publisher = mock(ChatRealtimeFanoutPublisher.class);
        ChatRealtimeEventListener listener = new ChatRealtimeEventListener(new ObjectMapper().findAndRegisterModules(), publisher);
        Sinks.Empty<Void> completion = Sinks.empty();
        String payload = """
                {"eventId":"event-1","type":"MESSAGE_CREATED","conversationId":"conversation-1",
                 "actorId":"user-1","recipientIds":["user-2"],"occurredAt":"2026-07-30T00:00:00Z"}
                """;
        when(publisher.publish(payload)).thenReturn(completion.asMono());

        CompletableFuture<Void> result = listener.handle(new ConsumerRecord<>(
                "chat_message_created", 0, 0, "conversation-1", payload));

        assertThat(result).isNotDone();
        completion.tryEmitEmpty();
        result.join();
        verify(publisher).publish(payload);
    }
}
