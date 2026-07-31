package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.StoryContextResponse;
import com.dauducbach.clone.modules.chat.service.ChatNotificationQueryService;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.entity.NotificationTemplates;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChatMessageNotificationListenerTest {

    @Test
    void storyReplyUsesDedicatedBodyAndRetainsExactChatDestinationMetadata() throws Exception {
        NotificationTemplatesRepository templates = mock(NotificationTemplatesRepository.class);
        PushNotificationService push = mock(PushNotificationService.class);
        ChatNotificationQueryService query = mock(ChatNotificationQueryService.class);
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        ChatMessageNotificationListener listener = new ChatMessageNotificationListener(
                objectMapper, templates, push, query);
        ChatMessageResponse message = new ChatMessageResponse(
                "message-1", "conversation-1", 7L, "client-1",
                "actor-1", "An", null, MessageType.STORY_REPLY, "Xin chào", null,
                null, null, Instant.parse("2026-07-31T00:00:00Z"), null, false,
                new StoryContextResponse(
                        "story-1", "owner-1", "IMAGE", 0L,
                        Instant.parse("2026-08-01T00:00:00Z"), true, "https://host/story.jpg"));
        ChatEvent event = ChatEvent.messageCreated(message, List.of("owner-1"));
        when(query.canReceiveMessageNotification(any(), any(), any())).thenReturn(Mono.just(true));
        when(templates.findByActionType(UserActionType.SEND_MESSAGE)).thenReturn(Mono.just(
                NotificationTemplates.builder().actionType(UserActionType.SEND_MESSAGE).template("ignored").build()));
        when(push.sendPushNotification(any())).thenReturn(Mono.just("notification-1"));

        listener.handle(new ConsumerRecord<>("chat-message-created", 0, 0L,
                "conversation-1", objectMapper.writeValueAsString(event))).join();

        var captor = org.mockito.ArgumentCaptor.forClass(NotificationForService.class);
        verify(push).sendPushNotification(captor.capture());
        NotificationForService notification = captor.getValue();
        assertThat(notification.getHtmlContent()).isEqualTo("An đã trả lời tin của bạn: \"Xin chào\"");
        assertThat(notification.getMetadata())
                .containsEntry("CONVERSATION_ID", "conversation-1")
                .containsEntry("MESSAGE_ID", "message-1")
                .containsEntry("MESSAGE_SEQ", "7");
    }
}
