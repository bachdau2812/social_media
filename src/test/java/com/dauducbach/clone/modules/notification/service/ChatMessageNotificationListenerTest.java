package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.service.ChatNotificationQueryService;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.StoryContextResponse;
import com.dauducbach.clone.modules.chat.publicapi.ChatNotificationQuery;
import com.dauducbach.clone.modules.chat.publicapi.ChatNotificationConversation;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import com.dauducbach.clone.modules.notification.incoming.chat.message.ChatMessageNotificationListener;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.delivery.DeliverNotificationUseCase;
import com.dauducbach.clone.modules.notification.delivery.NotificationContentNormalizer;
import com.dauducbach.clone.modules.notification.delivery.NotificationDestinationBuilder;
import com.dauducbach.clone.modules.notification.delivery.NotificationMetadataCodec;
import com.dauducbach.clone.modules.notification.delivery.NotificationPersistence;
import com.dauducbach.clone.modules.notification.delivery.NotificationPushGateway;
import com.dauducbach.clone.modules.notification.delivery.NotificationPushPayload;
import com.dauducbach.clone.modules.notification.delivery.NotificationPushPayloadFactory;
import com.dauducbach.clone.modules.notification.delivery.NotificationPushTokenQuery;
import com.dauducbach.clone.modules.notification.delivery.NotificationRealtimePublisher;
import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import com.dauducbach.clone.modules.notification.entity.NotificationTemplates;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

class ChatMessageNotificationListenerTest {

    @Test
    void groupMessageUsesGroupTitleAndGroupSpecificBody() throws Exception {
        NotificationTemplatesRepository templates = mock(NotificationTemplatesRepository.class);
        PushNotificationService push = mock(PushNotificationService.class);
        ConversationRepository conversations = mock(ConversationRepository.class);
        ConversationMemberRepository members = mock(ConversationMemberRepository.class);
        ChatNotificationQuery query = new ChatNotificationQueryService(conversations, members);
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        ChatMessageNotificationListener listener = new ChatMessageNotificationListener(
                mapper, templates, push, query, mock(UserIdentityQuery.class));
        ChatMessageResponse message = new ChatMessageResponse(
                "message-group", "conversation-group", 9L, "client-group",
                "actor-1", "An", null, MessageType.TEXT, "Xin chào", null,
                null, null, Instant.parse("2026-10-07T00:00:00Z"), null, false, null);
        when(conversations.findById("conversation-group")).thenReturn(Mono.just(Conversation.builder()
                .id("conversation-group").conversationType(ConversationType.GROUP).title("Nhóm dự án").build()));
        when(members.findActive("conversation-group", "recipient-1"))
                .thenReturn(Mono.just(ConversationMember.builder().build()));
        when(templates.findByActionType(UserActionType.SEND_MESSAGE)).thenReturn(Mono.empty());
        when(push.sendPushNotification(any())).thenReturn(Mono.just("notification-group"));

        listener.handle(new ConsumerRecord<>("chat.message.created", 0, 2L, "conversation-group",
                mapper.writeValueAsString(ChatEvent.messageCreated(message, List.of("recipient-1"))))).join();

        var captor = org.mockito.ArgumentCaptor.forClass(NotificationForService.class);
        verify(push).sendPushNotification(captor.capture());
        assertThat(captor.getValue().getTitle()).isEqualTo("Nhóm dự án");
        assertThat(captor.getValue().getHtmlContent()).isEqualTo("An đã gửi một tin nhắn: \"Xin chào\"");
    }

    @Test
    void storyReplyUsesDedicatedBodyAndRetainsExactChatDestinationMetadata() throws Exception {
        NotificationTemplatesRepository templates = mock(NotificationTemplatesRepository.class);
        PushNotificationService push = mock(PushNotificationService.class);
        ChatNotificationQuery query = mock(ChatNotificationQuery.class);
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        ChatMessageNotificationListener listener = new ChatMessageNotificationListener(
                objectMapper, templates, push, query, mock(UserIdentityQuery.class));
        ChatMessageResponse message = new ChatMessageResponse(
                "message-1", "conversation-1", 7L, "client-1",
                "actor-1", "An", null, MessageType.STORY_REPLY, "Xin chào", null,
                null, null, Instant.parse("2026-07-31T00:00:00Z"), null, false,
                new StoryContextResponse(
                        "story-1", "owner-1", "IMAGE", 0L,
                        Instant.parse("2026-08-01T00:00:00Z"), true, "https://host/story.jpg"));
        ChatEvent event = ChatEvent.messageCreated(message, List.of("owner-1"));
        when(query.canReceiveMessageNotification(any(), any(), any())).thenReturn(Mono.just(true));
        when(query.findConversation("conversation-1")).thenReturn(Mono.just(new ChatNotificationConversation(false, "")));
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

    @Test
    void persistenceFailureEscapesListenerSoKafkaCanRetry() throws Exception {
        NotificationTemplatesRepository templates = mock(NotificationTemplatesRepository.class);
        PushNotificationService push = mock(PushNotificationService.class);
        ChatNotificationQuery query = mock(ChatNotificationQuery.class);
        ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
        ChatMessageNotificationListener listener = new ChatMessageNotificationListener(
                objectMapper, templates, push, query, mock(UserIdentityQuery.class));
        ChatMessageResponse message = new ChatMessageResponse(
                "message-2", "conversation-1", 8L, "client-2",
                "actor-1", "An", null, MessageType.TEXT, "hi", null,
                null, null, Instant.parse("2026-08-01T00:00:00Z"), null, false,
                null);
        ChatEvent event = ChatEvent.messageCreated(message, List.of("owner-1"));
        when(query.canReceiveMessageNotification(any(), any(), any())).thenReturn(Mono.just(true));
        when(query.findConversation("conversation-1")).thenReturn(Mono.just(new ChatNotificationConversation(false, "")));
        when(templates.findByActionType(UserActionType.SEND_MESSAGE)).thenReturn(Mono.empty());
        when(push.sendPushNotification(any())).thenReturn(Mono.error(new IllegalStateException("database unavailable")));

        assertThatThrownBy(() -> listener.handle(new ConsumerRecord<>(
                "chat.message.created", 0, 1L, "conversation-1", objectMapper.writeValueAsString(event))).join())
                .hasRootCauseMessage("database unavailable");
    }

    @Test
    void groupSenderNameIsResolvedOnceAcrossRecipientsWhenEventContainsNoName() throws Exception {
        Fixture fixture = new Fixture(true, null, "Xin chào", MessageType.TEXT);
        when(fixture.identities.resolveDisplayName("actor-1")).thenReturn(Mono.just("Nguyễn An"));

        fixture.handle(List.of("actor-1", "recipient-1", "recipient-2"));

        var captor = org.mockito.ArgumentCaptor.forClass(NotificationForService.class);
        verify(fixture.push, times(2)).sendPushNotification(captor.capture());
        assertThat(captor.getAllValues()).allSatisfy(notification -> {
            assertThat(notification.getTitle()).isEqualTo("Nhóm dự án");
            assertThat(notification.getHtmlContent()).isEqualTo("Nguyễn An đã gửi một tin nhắn: \"Xin chào\"");
            assertThat(notification.getActorId()).isEqualTo("actor-1");
            assertThat(notification.getMetadata())
                    .containsEntry("GROUP_NAME", "Nhóm dự án")
                    .containsEntry("CONVERSATION_ID", "conversation-1")
                    .containsEntry("MESSAGE_ID", "message-1")
                    .containsEntry("MESSAGE_SEQ", "7");
        });
        verify(fixture.identities).resolveDisplayName("actor-1");
        verify(fixture.query).findConversation("conversation-1");
    }

    @Test
    void senderIdInDisplayNameIsReplacedWithResolvedName() throws Exception {
        Fixture fixture = new Fixture(true, "actor-1", "hi", MessageType.TEXT);
        when(fixture.identities.resolveDisplayName("actor-1")).thenReturn(Mono.just("an.nguyen"));

        fixture.handle(List.of("recipient-1"));

        var captor = org.mockito.ArgumentCaptor.forClass(NotificationForService.class);
        verify(fixture.push).sendPushNotification(captor.capture());
        assertThat(captor.getValue().getHtmlContent()).isEqualTo("an.nguyen đã gửi một tin nhắn: \"hi\"");
    }

    @Test
    void missingProfileNeverDisplaysSenderId() throws Exception {
        Fixture fixture = new Fixture(true, null, "hi", MessageType.TEXT);
        when(fixture.identities.resolveDisplayName("actor-1")).thenReturn(Mono.just("actor-1"));

        fixture.handle(List.of("recipient-1"));

        var captor = org.mockito.ArgumentCaptor.forClass(NotificationForService.class);
        verify(fixture.push).sendPushNotification(captor.capture());
        assertThat(captor.getValue().getHtmlContent()).isEqualTo("Người dùng đã gửi một tin nhắn: \"hi\"");
    }

    @Test
    void groupImageWithoutCaptionUsesPreviewWithoutPersonalRecipientWording() throws Exception {
        Fixture fixture = new Fixture(true, "An", null, MessageType.IMAGE);

        fixture.handle(List.of("recipient-1"));

        var captor = org.mockito.ArgumentCaptor.forClass(NotificationForService.class);
        verify(fixture.push).sendPushNotification(captor.capture());
        assertThat(captor.getValue().getHtmlContent()).isEqualTo("An đã gửi một tin nhắn: \"Đã gửi một ảnh\"");
    }

    @Test
    void directMessageRetainsSenderTitleAndPersonalRecipientWording() throws Exception {
        Fixture fixture = new Fixture(false, null, "hi", MessageType.TEXT);
        when(fixture.identities.resolveDisplayName("actor-1")).thenReturn(Mono.just("An"));

        fixture.handle(List.of("recipient-1"));

        var captor = org.mockito.ArgumentCaptor.forClass(NotificationForService.class);
        verify(fixture.push).sendPushNotification(captor.capture());
        assertThat(captor.getValue().getTitle()).isEqualTo("An");
        assertThat(captor.getValue().getHtmlContent()).isEqualTo("An đã gửi cho bạn một tin nhắn mới: “hi”");
        assertThat(captor.getValue().getMetadata()).doesNotContainKey("GROUP_NAME");
    }

    @Test
    void mutedOrInactiveRecipientDoesNotTriggerDisplayLookupsOrPush() throws Exception {
        Fixture fixture = new Fixture(true, null, "hi", MessageType.TEXT);
        when(fixture.query.canReceiveMessageNotification(any(), any(), any())).thenReturn(Mono.just(false));

        fixture.handle(List.of("recipient-1"));

        verify(fixture.query, never()).findConversation(any());
        verify(fixture.identities, never()).resolveDisplayName(any());
        verify(fixture.push, never()).sendPushNotification(any());
    }

    @Test
    void nameLookupFailureEscapesListenerForKafkaRetry() {
        Fixture fixture = new Fixture(true, null, "hi", MessageType.TEXT);
        when(fixture.identities.resolveDisplayName("actor-1"))
                .thenReturn(Mono.error(new IllegalStateException("profile unavailable")));

        assertThatThrownBy(() -> fixture.handle(List.of("recipient-1")))
                .hasRootCauseMessage("profile unavailable");
        verify(fixture.push, never()).sendPushNotification(any());
    }

    @Test
    void groupContentSurvivesPersistenceAndFcmPayloadWithExactMessageDestination() throws Exception {
        Fixture fixture = new Fixture(true, null, "Xin chào", MessageType.TEXT);
        when(fixture.identities.resolveDisplayName("actor-1")).thenReturn(Mono.just("An"));
        NotificationPersistence persistence = mock(NotificationPersistence.class);
        NotificationPushTokenQuery tokens = mock(NotificationPushTokenQuery.class);
        NotificationPushGateway gateway = mock(NotificationPushGateway.class);
        NotificationRealtimePublisher realtime = mock(NotificationRealtimePublisher.class);
        when(persistence.persistIfNew(any(), any())).thenReturn(Mono.just(true));
        when(tokens.findDeviceToken("recipient-1")).thenReturn(Mono.just("device-token"));
        when(gateway.send(any())).thenReturn(Mono.just("fcm-message-1"));
        when(realtime.notifyChanged(any(), any())).thenReturn(Mono.empty());
        PushNotificationService push = new PushNotificationService(new DeliverNotificationUseCase(
                persistence, tokens, gateway, new NotificationDestinationBuilder(),
                new NotificationContentNormalizer(), new NotificationMetadataCodec(fixture.mapper),
                new NotificationPushPayloadFactory(), realtime));
        ChatMessageNotificationListener listener = new ChatMessageNotificationListener(
                fixture.mapper, fixture.templates, push, fixture.query, fixture.identities);
        ChatEvent event = ChatEvent.messageCreated(fixture.message, List.of("recipient-1"));

        listener.handle(new ConsumerRecord<>("chat.message.created", 0, 0L, "conversation-1",
                fixture.mapper.writeValueAsString(event))).join();

        var stored = org.mockito.ArgumentCaptor.forClass(NotificationEvents.class);
        verify(persistence).persistIfNew(stored.capture(), any());
        assertThat(stored.getValue().getContent()).isEqualTo("An đã gửi một tin nhắn: \"Xin chào\"");
        assertThat(stored.getValue().getDeepLink())
                .isEqualTo("/messages?conversationId=conversation-1&messageId=message-1&messageSeq=7");
        var payload = org.mockito.ArgumentCaptor.forClass(NotificationPushPayload.class);
        verify(gateway).send(payload.capture());
        assertThat(payload.getValue().title()).isEqualTo("Nhóm dự án");
        assertThat(payload.getValue().body()).isEqualTo(stored.getValue().getContent());
        assertThat(payload.getValue().data()).containsEntry("url", stored.getValue().getDeepLink());
    }

    @Test
    void emptyNameLookupUsesHumanReadableFallback() throws Exception {
        Fixture fixture = new Fixture(true, "  ", "hi", MessageType.TEXT);
        when(fixture.identities.resolveDisplayName("actor-1")).thenReturn(Mono.empty());

        fixture.handle(List.of("recipient-1"));

        var captor = org.mockito.ArgumentCaptor.forClass(NotificationForService.class);
        verify(fixture.push).sendPushNotification(captor.capture());
        assertThat(captor.getValue().getHtmlContent()).startsWith("Người dùng đã gửi một tin nhắn:");
    }

    @Test
    void absentConversationDoesNotSendMisleadingNotification() throws Exception {
        Fixture fixture = new Fixture(true, "An", "hi", MessageType.TEXT);
        when(fixture.query.findConversation("conversation-1")).thenReturn(Mono.empty());

        fixture.handle(List.of("recipient-1"));

        verify(fixture.push, never()).sendPushNotification(any());
    }

    private static class Fixture {
        final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        final NotificationTemplatesRepository templates = mock(NotificationTemplatesRepository.class);
        final PushNotificationService push = mock(PushNotificationService.class);
        final ChatNotificationQuery query = mock(ChatNotificationQuery.class);
        final UserIdentityQuery identities = mock(UserIdentityQuery.class);
        final ChatMessageNotificationListener listener = new ChatMessageNotificationListener(
                mapper, templates, push, query, identities);
        final ChatMessageResponse message;

        Fixture(boolean group, String senderName, String content, MessageType type) {
            message = new ChatMessageResponse("message-1", "conversation-1", 7L, "client-1",
                    "actor-1", senderName, null, type, content, null, null, null,
                    Instant.parse("2026-10-07T00:00:00Z"), null, false, null);
            when(query.canReceiveMessageNotification(any(), any(), any())).thenReturn(Mono.just(true));
            when(query.findConversation("conversation-1"))
                    .thenReturn(Mono.just(new ChatNotificationConversation(group, "Nhóm dự án")));
            when(templates.findByActionType(UserActionType.SEND_MESSAGE)).thenReturn(Mono.empty());
            when(push.sendPushNotification(any())).thenReturn(Mono.just("notification-1"));
        }

        void handle(List<String> recipients) throws Exception {
            listener.handle(new ConsumerRecord<>("chat.message.created", 0, 0L, "conversation-1",
                    mapper.writeValueAsString(ChatEvent.messageCreated(message, recipients)))).join();
        }
    }
}
