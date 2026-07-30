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
public class ChatMemberRequestNotificationListener {
    private static final Logger log = LoggerFactory.getLogger(ChatMemberRequestNotificationListener.class);

    private final ObjectMapper objectMapper;
    private final NotificationTemplatesRepository templateRepository;
    private final PushNotificationService pushNotificationService;
    private final UserIdentityQueryService userIdentityQueryService;
    private final ChatNotificationQueryService chatNotificationQueryService;

    @KafkaListener(topics = KafkaChatEventPublisher.MEMBER_REQUESTED_TOPIC, groupId = "chat-member-request-notification-service")
    public CompletableFuture<Void> handle(ConsumerRecord<String, String> record) {
        try {
            ChatEvent event = objectMapper.readValue(record.value(), ChatEvent.class);
            if (event.type() != ChatEventType.MEMBER_REQUESTED || !event.conversationId().equals(record.key())) return CompletableFuture.completedFuture(null);
            Mono<String> actorName = userIdentityQueryService.resolveDisplayName(event.actorId());
            Mono<String> targetName = userIdentityQueryService.resolveDisplayName(event.targetUserId());
            Mono<String> groupName = chatNotificationQueryService.getConversationTitle(event.conversationId(), "Nhóm chat");
            Mono<String> template = templateRepository.findByActionType(UserActionType.CHAT_MEMBER_REQUEST)
                    .map(item -> item.getTemplate())
                    .defaultIfEmpty("{ACTOR} đề xuất thêm {TARGET} vào nhóm");

            return Mono.zip(actorName, targetName, groupName, template)
                    .flatMapMany(tuple -> Flux.fromIterable(event.recipientIds())
                            .filter(recipient -> !recipient.equals(event.actorId()))
                            .concatMap(recipient -> pushNotificationService.sendPushNotification(
                                    NotificationForService.builder()
                                            .actorId(event.actorId())
                                            .actionType(UserActionType.CHAT_MEMBER_REQUEST)
                                            .entityId(event.entityId())
                                            .entityType("CHAT_MEMBER_REQUEST")
                                            .recipient(recipient)
                                            .title("Yêu cầu thêm thành viên")
                                            .htmlContent("Nh\u00f3m " + tuple.getT3() + " c\u1ee7a b\u1ea1n c\u00f3 y\u00eau c\u1ea7u tham gia m\u1edbi")
                                            .metadata(groupRequestMetadata(event, tuple.getT3()))
                                            .notificationType(NotificationType.PUSH)
                                            .build())))
                    .then()
                    .doOnError(error -> log.error("|ChatMemberRequestNotificationListener|handle|failed|conversationId={}|error={}", event.conversationId(), error.getMessage()))
                    .onErrorResume(error -> Mono.empty())
                    .toFuture();
        } catch (Exception error) {
            log.error("|ChatMemberRequestNotificationListener|handle|invalid payload|key={}|error={}", record.key(), error.getMessage());
            return CompletableFuture.completedFuture(null);
        }
    }

    private Map<String, String> groupRequestMetadata(ChatEvent event, String groupName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("EVENT_ID", event.eventId());
        metadata.put("CONVERSATION_ID", event.conversationId());
        metadata.put("REQUEST_ID", event.entityId());
        metadata.put("TARGET_USER_ID", event.targetUserId());
        metadata.put("GROUP_NAME", groupName);
        return metadata;
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
        for (String value : values) if (value != null && !value.isBlank()) return value;
        return "Thành viên";
    }
}
