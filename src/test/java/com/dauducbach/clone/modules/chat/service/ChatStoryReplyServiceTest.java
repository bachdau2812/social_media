package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.ConversationResponse;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChatStoryReplyServiceTest {

    @Test
    void reusesDirectConversationAndSendsTypedStoryReply() {
        ConversationService conversations = mock(ConversationService.class);
        SendMessageService messages = mock(SendMessageService.class);
        ChatStoryReplyService service = new ChatStoryReplyService(conversations, messages);
        StoryReplyMessaging.StoryReplyCommand command = new StoryReplyMessaging.StoryReplyCommand(
                "sender-1", "story-1", "owner-1", "hello", "client-1",
                "VIDEO", 12400L, Instant.parse("2026-08-01T00:00:00Z"));
        ConversationResponse conversation = mock(ConversationResponse.class);
        when(conversation.id()).thenReturn("conversation-1");
        ChatMessageResponse sent = mock(ChatMessageResponse.class);
        when(conversations.createDirect(any(), any())).thenReturn(Mono.just(conversation));
        when(messages.sendMessage(any(), any(), any())).thenReturn(Mono.just(sent));

        StepVerifier.create(service.send(command)).expectNext(sent).verifyComplete();

        var directRequest = org.mockito.ArgumentCaptor.forClass(
                com.dauducbach.clone.modules.chat.dto.request.CreateDirectConversationRequest.class);
        verify(conversations).createDirect(org.mockito.ArgumentMatchers.eq("sender-1"), directRequest.capture());
        assertThat(directRequest.getValue().targetUserId()).isEqualTo("owner-1");

        var messageRequest = org.mockito.ArgumentCaptor.forClass(SendMessageRequest.class);
        verify(messages).sendMessage(
                org.mockito.ArgumentMatchers.eq("sender-1"),
                org.mockito.ArgumentMatchers.eq("conversation-1"),
                messageRequest.capture());
        assertThat(messageRequest.getValue().messageType()).isEqualTo(MessageType.STORY_REPLY);
        assertThat(messageRequest.getValue().storyContext().previewAtMs()).isEqualTo(12400L);
        assertThat(messageRequest.getValue().recipientId()).isEqualTo("owner-1");
    }
}
