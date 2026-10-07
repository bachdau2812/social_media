package com.dauducbach.clone.modules.chat.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ChatRealtimeLocalDispatcherTest {

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"created", "forwarded", "deleted"})
    void productionMessageFanoutUsesCurrentVisibleMembers(String variant) throws Exception {
        var mapper = new ObjectMapper().findAndRegisterModules();
        var registry = mock(ChatSessionRegistry.class);
        var reactions = mock(com.dauducbach.clone.modules.chat.repository.MessageReactionRepository.class);
        var members = mock(com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository.class);
        var dispatcher = new ChatRealtimeLocalDispatcher(mapper, registry, reactions, members);
        var stored = com.dauducbach.clone.modules.chat.entity.ChatMessage.builder()
                .id("m").conversationId("c").senderId("sender").messageSeq(5)
                .messageType(com.dauducbach.clone.modules.chat.constant.MessageType.TEXT)
                .content("private").forwarded("forwarded".equals(variant))
                .deletedAt("deleted".equals(variant) ? java.time.Instant.now() : null).build();
        var response = new ChatResponseMapper().toChatMessageResponse(stored);
        var addressed = java.util.List.of("active", "removed", "hidden");
        var event = "deleted".equals(variant)
                ? com.dauducbach.clone.modules.chat.publicapi.ChatEvent.messageDeleted(response, "sender", addressed)
                : com.dauducbach.clone.modules.chat.publicapi.ChatEvent.messageCreated(response, addressed);
        var payload = mapper.writeValueAsString(event);
        org.mockito.Mockito.when(members.findVisibleActiveMembers("c", 5)).thenReturn(reactor.core.publisher.Flux.just(
                com.dauducbach.clone.modules.chat.entity.ConversationMember.builder().userId("active").joinedSeq(1).build(),
                com.dauducbach.clone.modules.chat.entity.ConversationMember.builder().userId("not-addressed").joinedSeq(1).build()));
        StepVerifier.create(dispatcher.dispatch(payload)).verifyComplete();
        verify(members).findVisibleActiveMembers("c", 5);
        verify(registry).sendToUser("active", payload);
        org.mockito.Mockito.verifyNoMoreInteractions(registry);
        org.mockito.Mockito.verifyNoInteractions(reactions);
    }

    @Test
    void ordinaryMessageFanoutAlsoRechecksMembershipAndHistory() throws Exception {
        var mapper = new ObjectMapper().findAndRegisterModules();
        var registry = mock(ChatSessionRegistry.class);
        var reactions = mock(com.dauducbach.clone.modules.chat.repository.MessageReactionRepository.class);
        var dispatcher = new ChatRealtimeLocalDispatcher(mapper, registry, reactions);
        var message = com.dauducbach.clone.modules.chat.entity.ChatMessage.builder()
                .id("m").conversationId("c").senderId("sender").messageSeq(5)
                .messageType(com.dauducbach.clone.modules.chat.constant.MessageType.TEXT).content("private").build();
        var event = com.dauducbach.clone.modules.chat.publicapi.ChatEvent.messageCreated(
                new ChatResponseMapper().toChatMessageResponse(message), java.util.List.of("active", "removed", "hidden"));
        var payload = mapper.writeValueAsString(event);
        org.mockito.Mockito.when(reactions.eligibleRecipients("c", 5))
                .thenReturn(reactor.core.publisher.Flux.just("active", "not-addressed"));
        StepVerifier.create(dispatcher.dispatch(payload)).verifyComplete();
        verify(registry).sendToUser("active", payload);
        org.mockito.Mockito.verifyNoMoreInteractions(registry);
    }

    @Test
    void reactionFanoutRechecksCurrentMembershipAndHistoryVisibility() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ChatSessionRegistry registry = mock(ChatSessionRegistry.class);
        var reactions = mock(com.dauducbach.clone.modules.chat.repository.MessageReactionRepository.class);
        var dispatcher = new ChatRealtimeLocalDispatcher(mapper, registry, reactions);
        var event = com.dauducbach.clone.modules.chat.publicapi.ChatEvent.reactionChanged("c",
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
