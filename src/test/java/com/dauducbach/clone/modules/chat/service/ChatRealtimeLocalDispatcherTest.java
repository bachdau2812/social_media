package com.dauducbach.clone.modules.chat.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ChatRealtimeLocalDispatcherTest {

    @Test
    void dispatchesOnlyToLocalRecipientSessions() {
        ChatSessionRegistry registry = mock(ChatSessionRegistry.class);
        ChatRealtimeLocalDispatcher dispatcher = new ChatRealtimeLocalDispatcher(
                new ObjectMapper().findAndRegisterModules(), registry);
        String payload = """
                {"eventId":"event-1","type":"MESSAGE_CREATED","conversationId":"conversation-1",
                 "actorId":"user-1","recipientIds":["user-2","user-3"],"occurredAt":"2026-07-30T00:00:00Z"}
                """;

        StepVerifier.create(dispatcher.dispatch(payload)).verifyComplete();

        verify(registry).sendToUser("user-2", payload);
        verify(registry).sendToUser("user-3", payload);
        verify(registry, never()).sendToUser("user-1", payload);
    }
}
