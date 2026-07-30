package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.chat.constant.ChatEventType;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.service.ChatNotificationQueryService;
import com.dauducbach.clone.modules.chat.service.KafkaChatEventPublisher;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.dauducbach.clone.modules.user.service.UserIdentityQueryService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class ChatMembershipNotificationListener {
    private static final Logger log = LoggerFactory.getLogger(ChatMembershipNotificationListener.class);

    private final ObjectMapper objectMapper;
    private final ChatNotificationQueryService chatNotificationQueryService;
    private final UserIdentityQueryService userIdentityQueryService;
    private final NotificationTemplatesRepository templateRepository;
    private final PushNotificationService pushNotificationService;

    @KafkaListener(
            topics = KafkaChatEventPublisher.MEMBERSHIP_CHANGED_TOPIC,
            groupId = "chat-membership-notification-service")
    public CompletableFuture<Void> handle(ConsumerRecord<String, String> record) {
        try {
            ChatEvent event = objectMapper.readValue(record.value(), ChatEvent.class);
            if (!event.conversationId().equals(record.key())
                    || (event.type() != ChatEventType.GROUP_CREATED
                    && event.type() != ChatEventType.MEMBER_ADDED)) {
                return CompletableFuture.completedFuture(null);
            }

            return Mono.zip(
                            chatNotificationQueryService.getConversationTitle(event.conversationId(), "Nhóm chat"),
                            identity(event.actorId()),
                            templateRepository.findByActionType(UserActionType.CHAT_GROUP_MEMBER_ADDED)
                                    .map(template -> template.getTemplate())
                                    .defaultIfEmpty("{ACTOR} đã thêm bạn vào nhóm {GROUP}"))
                    .flatMapMany(tuple -> Flux.fromIterable(event.recipientIds())
                            .distinct()
                            .filter(recipientId -> !recipientId.equals(event.actorId()))
                            .concatMap(recipientId -> chatNotificationQueryService.isActiveMember(event.conversationId(), recipientId)
                                    .filter(Boolean::booleanValue)
                                    .flatMap(ignored -> pushNotificationService.sendPushNotification(
                                            NotificationForService.builder()
                                                    .actorId(event.actorId())
                                                    .actionType(UserActionType.CHAT_GROUP_MEMBER_ADDED)
                                                    .entityId(event.conversationId())
                                                    .entityType("CHAT_CONVERSATION")
                                                    .recipient(recipientId)
                                                    .title("Đã được thêm vào nhóm")
                                                    .htmlContent(tuple.getT3()
                                                            .replace("{ACTOR}", tuple.getT2())
                                                            .replace("{GROUP}", tuple.getT1()))
                                                    .metadata(membershipMetadata(event, tuple.getT1()))
                                                    .notificationType(NotificationType.PUSH)
                                                    .build()))))
                    .then()
                    .doOnError(error -> log.error(
                            "|ChatMembershipNotificationListener|handle|failed|conversationId={}|error={}",
                            event.conversationId(), error.getMessage()))
                    .onErrorResume(error -> Mono.empty())
                    .toFuture();
        } catch (Exception error) {
            log.error(
                    "|ChatMembershipNotificationListener|handle|invalid payload|key={}|error={}",
                    record.key(), error.getMessage());
            return CompletableFuture.completedFuture(null);
        }
    }

    private String membershipContent(String actorName, String groupName) {
        return actorName + " \u0111\u00e3 tham gia nh\u00f3m " + groupName;
    }

    private Map<String, String> membershipMetadata(ChatEvent event, String groupName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("EVENT_ID", event.eventId());
        metadata.put("CONVERSATION_ID", event.conversationId());
        metadata.put("GROUP_NAME", groupName);
        if (event.targetUserId() != null) {
            metadata.put("TARGET_USER_ID", event.targetUserId());
        }
        return metadata;
    }

    private Mono<String> identity(String userId) {
        return userIdentityQueryService.resolveDisplayName(userId);
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "Th\u00e0nh vi\u00ean";
    }

    private String firstNonBlankLegacy(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "Thành viên";
    }
}
