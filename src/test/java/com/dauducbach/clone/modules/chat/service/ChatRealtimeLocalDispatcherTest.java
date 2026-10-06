package com.dauducbach.clone.modules.chat.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ChatRealtimeLocalDispatcherTest {

    @Test
    void reactionFanoutRechecksCurrentMembershipAndHistoryVisibility() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ChatSessionRegistry registry = mock(ChatSessionRegistry.class);
        var reactions = mock(com.dauducbach.clone.modules.chat.repository.MessageReactionRepository.class);
        var dispatcher = new ChatRealtimeLocalDispatcher(mapper, registry, reactions);
        var event = com.dauducbach.clone.modules.chat.dto.event.ChatEvent.reactionChanged("c",
                new com.dauducbach.clone.modules.chat.dto.response.ReactionState("m", 5, 1, "me", null, 0, java.util.List.of()),
                java.util.List.of("me", "removed", "hidden"));
        String payload = mapper.writeValueAsString(event);
        org.mockito.Mockito.when(reactions.eligibleRecipients("c", 5))
                .thenReturn(reactor.core.publisher.Flux.just("me", "joined-later"));
        reactor.test.StepVerifier.create(dispatcher.dispatch(payload)).verifyComplete();
        verify(registry).sendToUser("me", payload);
        org.mockito.Mockito.verifyNoMoreInteractions(registry);
    }

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
