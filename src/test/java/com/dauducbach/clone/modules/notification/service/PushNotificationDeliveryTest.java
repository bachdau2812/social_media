package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import com.dauducbach.clone.modules.notification.entity.NotificationPushToken;
import com.dauducbach.clone.modules.notification.entity.UserNotifications;
import com.dauducbach.clone.modules.notification.repository.NotificationEventsRepository;
import com.dauducbach.clone.modules.notification.repository.UserPushNotificationRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PushNotificationDeliveryTest {
    private final UserPushNotificationRepository tokenRepository = mock(UserPushNotificationRepository.class);
    private final NotificationEventsRepository eventRepository = mock(NotificationEventsRepository.class);
    private final R2dbcEntityTemplate entityTemplate = mock(R2dbcEntityTemplate.class);
    private final NotificationPushGateway pushGateway = mock(NotificationPushGateway.class);
    private final NotificationSseService realtime = mock(NotificationSseService.class);

    @Test
    void persistsInAppNotificationWhenRecipientHasNoPushToken() {
        PushNotificationService service = newService();
        mockNotificationInserts(new ArrayList<>());
        when(tokenRepository.findByUserId("recipient-1")).thenReturn(Mono.empty());

        StepVerifier.create(service.sendPushNotification(notificationRequest()))
                .expectNextMatches(result -> result.contains("persisted"))
                .verifyComplete();

        verify(entityTemplate).insert(NotificationEvents.class);
        verify(entityTemplate).insert(UserNotifications.class);
        verify(realtime).notifyChanged(eq("recipient-1"), anyString());
        verifyNoInteractions(pushGateway);
    }

    @Test
    void persistsBeforePushAndKeepsNotificationWhenFcmFails() {
        List<String> order = new ArrayList<>();
        PushNotificationService service = newService();
        mockNotificationInserts(order);
        when(tokenRepository.findByUserId("recipient-1"))
                .thenReturn(Mono.just(NotificationPushToken.builder()
                        .id("token-id")
                        .userId("recipient-1")
                        .deviceToken("device-token")
                        .createdAt(Instant.now())
                        .build()));
        when(pushGateway.send(any())).thenAnswer(invocation -> {
            order.add("push");
            return Mono.error(new IllegalStateException("FCM unavailable"));
        });
        when(realtime.notifyChanged(eq("recipient-1"), anyString())).thenAnswer(invocation -> Mono.fromRunnable(() -> order.add("realtime")));

        StepVerifier.create(service.sendPushNotification(notificationRequest()))
                .expectNextMatches(result -> result.contains("persisted"))
                .verifyComplete();

        assertThat(order).containsExactly("event", "user-notification", "realtime", "push");
        verify(eventRepository, never()).deleteById(any(String.class));
    }

    @Test
    void pushPayloadContainsCanonicalDataAndStableDedupTag() {
        FirebasePushMessageFactory factory = new FirebasePushMessageFactory();

        NotificationPushPayload payload = factory.create(
                "device-token",
                "notification-1",
                "SEND_MESSAGE:recipient-1:message-1",
                "/messages?conversationId=conversation-1&messageId=message-1",
                notificationRequest());

        assertThat(payload.data()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "notificationId", "notification-1",
                "actionType", "SEND_MESSAGE",
                "url", "/messages?conversationId=conversation-1&messageId=message-1"));
        assertThat(payload.tag()).isEqualTo("SEND_MESSAGE:recipient-1:message-1");
    }

    @Test
    void skipsDeliveryWhenPublicationDedupKeyAlreadyExists() {
        PushNotificationService service = newService();
        NotificationEvents existing = NotificationEvents.builder()
                .id("event-existing")
                .dedupKey("UP_STORY:publication-1:recipient-1")
                .build();
        when(eventRepository.findByDedupKey("UP_STORY:publication-1:recipient-1")).thenReturn(Mono.just(existing));

        NotificationForService request = notificationRequest();
        request.setActionType(UserActionType.UP_STORY);
        request.setEntityId("story-2");
        request.setEntityType("STORY");
        request.setDedupKey("UP_STORY:publication-1");

        StepVerifier.create(service.sendPushNotification(request))
                .expectNext("Duplicate notification skipped")
                .verifyComplete();

        verify(entityTemplate, never()).insert(NotificationEvents.class);
        verify(entityTemplate, never()).insert(UserNotifications.class);
        verifyNoInteractions(realtime);
        verifyNoInteractions(pushGateway);
    }

    @Test
    void skipsDeliveryWhenStoryLikeInteractionDedupKeyAlreadyExists() {
        PushNotificationService service = newService();
        NotificationEvents existing = NotificationEvents.builder()
                .id("event-existing")
                .dedupKey("LIKE_STORY:interaction-1:recipient-1")
                .build();
        when(eventRepository.findByDedupKey("LIKE_STORY:interaction-1:recipient-1"))
                .thenReturn(Mono.just(existing));

        NotificationForService request = notificationRequest();
        request.setActionType(UserActionType.LIKE_STORY);
        request.setEntityId("story-1");
        request.setEntityType("STORY");
        request.setDedupKey("LIKE_STORY:interaction-1");

        StepVerifier.create(service.sendPushNotification(request))
                .expectNext("Duplicate notification skipped")
                .verifyComplete();

        verify(entityTemplate, never()).insert(NotificationEvents.class);
        verify(entityTemplate, never()).insert(UserNotifications.class);
        verifyNoInteractions(realtime);
        verifyNoInteractions(pushGateway);
    }

    private PushNotificationService newService() {
        lenient().when(eventRepository.findByDedupKey(anyString())).thenReturn(Mono.empty());
        lenient().when(realtime.notifyChanged(anyString(), anyString())).thenReturn(Mono.empty());
        return new PushNotificationService(
                tokenRepository,
                eventRepository,
                entityTemplate,
                pushGateway,
                new NotificationDestinationBuilder(),
                new NotificationContentNormalizer(),
                new NotificationMetadataCodec(new ObjectMapper()),
                new FirebasePushMessageFactory(),
                realtime);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void mockNotificationInserts(List<String> order) {
        ReactiveInsertOperation.ReactiveInsert<NotificationEvents> eventInsert =
                mock(ReactiveInsertOperation.ReactiveInsert.class);
        ReactiveInsertOperation.ReactiveInsert<UserNotifications> userNotificationInsert =
                mock(ReactiveInsertOperation.ReactiveInsert.class);
        when(entityTemplate.insert(NotificationEvents.class)).thenReturn(eventInsert);
        when(entityTemplate.insert(UserNotifications.class)).thenReturn(userNotificationInsert);
        when(eventInsert.using(any(NotificationEvents.class))).thenAnswer(invocation -> Mono.defer(() -> {
            order.add("event");
            return Mono.just(invocation.getArgument(0));
        }));
        when(userNotificationInsert.using(any(UserNotifications.class))).thenAnswer(invocation -> Mono.defer(() -> {
            order.add("user-notification");
            return Mono.just(invocation.getArgument(0));
        }));
    }

    private NotificationForService notificationRequest() {
        return NotificationForService.builder()
                .actorId("actor-1")
                .actionType(UserActionType.SEND_MESSAGE)
                .entityId("message-1")
                .entityType("CHAT_MESSAGE")
                .recipient("recipient-1")
                .title("Tin nhắn mới")
                .htmlContent("A đã gửi cho bạn một tin nhắn mới")
                .metadata(Map.of("CONVERSATION_ID", "conversation-1"))
                .notificationType(NotificationType.PUSH)
                .build();
    }
}
