package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.chat.constant.ChatEventType;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.service.ChatNotificationQueryService;
import com.dauducbach.clone.modules.chat.service.KafkaChatEventPublisher;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class ChatMessageNotificationListener {
    private static final Logger log = LoggerFactory.getLogger(ChatMessageNotificationListener.class);

    private final ObjectMapper objectMapper;
    private final NotificationTemplatesRepository templateRepository;
    private final PushNotificationService pushNotificationService;
    private final ChatNotificationQueryService chatNotificationQueryService;

    @KafkaListener(
            topics = KafkaChatEventPublisher.MESSAGE_CREATED_TOPIC,
            groupId = "chat-notification-service")
    public CompletableFuture<Void> handle(ConsumerRecord<String, String> record) {
        try {
            ChatEvent event = objectMapper.readValue(record.value(), ChatEvent.class);
            if (event.type() != ChatEventType.MESSAGE_CREATED || event.message() == null) {
                return CompletableFuture.completedFuture(null);
            }
            if (event.message().messageType() == MessageType.SYSTEM) {
                return CompletableFuture.completedFuture(null);
            }
            if (!event.conversationId().equals(record.key())) {
                log.warn("|ChatMessageNotificationListener|handle|invalid key|key={}|conversationId={}",
                        record.key(), event.conversationId());
                return CompletableFuture.completedFuture(null);
            }

            return Flux.fromIterable(event.recipientIds())
                    .filter(recipientId -> !recipientId.equals(event.actorId()))
                    .concatMap(recipientId -> sendPush(recipientId, event))
                    .then()
                    .doOnError(error -> log.error(
                            "|ChatMessageNotificationListener|handle|failed|conversationId={}|error={}",
                            event.conversationId(), error.getMessage()))
                    .onErrorResume(error -> Mono.empty())
                    .toFuture();
        } catch (Exception error) {
            log.error("|ChatMessageNotificationListener|handle|invalid payload|key={}|error={}",
                    record.key(), error.getMessage());
            return CompletableFuture.completedFuture(null);
        }
    }

    private Mono<Void> sendPush(String recipientId, ChatEvent event) {
        return chatNotificationQueryService.canReceiveMessageNotification(event.conversationId(), recipientId, Instant.now())
                .filter(Boolean::booleanValue)
                .flatMap(ignored -> templateRepository.findByActionType(UserActionType.SEND_MESSAGE)
                        .map(template -> template.getTemplate())
                        .defaultIfEmpty("{USERNAME}: {MESSAGE}")
                        .flatMap(template -> {
                            ChatMessageResponse message = event.message();
                            String sender = firstNonBlank(message.senderDisplayName(), message.senderId(), "Người dùng");
                            String preview = previewEnhanced(message);
                            String body = notificationBody(sender, message, preview);
                            Map<String, String> metadata = new HashMap<>();
                            metadata.put("EVENT_ID", event.eventId());
                            metadata.put("CONVERSATION_ID", event.conversationId());
                            metadata.put("MESSAGE_ID", message.id());
                            metadata.put("MESSAGE_SEQ", String.valueOf(message.messageSeq()));
                            metadata.put("MESSAGE_TYPE", message.messageType().name());
                            metadata.put("MESSAGE_PREVIEW", preview);
                            return pushNotificationService.sendPushNotification(
                                    NotificationForService.builder()
                                            .actorId(event.actorId())
                                            .actionType(UserActionType.SEND_MESSAGE)
                                            .entityId(message.id())
                                            .entityType("CHAT_MESSAGE")
                                            .recipient(recipientId)
                                            .title(sender)
                                            .htmlContent(body)
                                            .metadata(metadata)
                                            .notificationType(NotificationType.PUSH)
                                            .build());
                        }))
                .then();
    }


    private String notificationBody(String sender, ChatMessageResponse message, String preview) {
        if (message.content() != null && !message.content().isBlank()) {
            return sender + " \u0111\u00e3 g\u1eedi cho b\u1ea1n m\u1ed9t tin nh\u1eafn m\u1edbi: \u201c" + preview + "\u201d";
        }
        return switch (message.messageType()) {
            case IMAGE -> sender + " \u0111\u00e3 g\u1eedi cho b\u1ea1n m\u1ed9t \u1ea3nh";
            case VIDEO -> sender + " \u0111\u00e3 g\u1eedi cho b\u1ea1n m\u1ed9t video";
            case AUDIO -> sender + " \u0111\u00e3 g\u1eedi cho b\u1ea1n m\u1ed9t tin nh\u1eafn tho\u1ea1i";
            case FILE -> sender + " \u0111\u00e3 g\u1eedi cho b\u1ea1n m\u1ed9t t\u1ec7p";
            default -> sender + " \u0111\u00e3 g\u1eedi cho b\u1ea1n m\u1ed9t tin nh\u1eafn m\u1edbi";
        };
    }
    private String previewEnhanced(ChatMessageResponse message) {
        if (message.content() != null && !message.content().isBlank()) {
            String content = message.content().trim();
            return content.length() > 120 ? content.substring(0, 117) + "..." : content;
        }
        return switch (message.messageType()) {
            case IMAGE -> "\u0110\u00e3 g\u1eedi m\u1ed9t \u1ea3nh";
            case VIDEO -> "\u0110\u00e3 g\u1eedi m\u1ed9t video";
            case AUDIO -> "\u0110\u00e3 g\u1eedi m\u1ed9t tin nh\u1eafn tho\u1ea1i";
            case FILE -> "\u0110\u00e3 g\u1eedi m\u1ed9t t\u1ec7p";
            default -> "\u0110\u00e3 g\u1eedi m\u1ed9t tin nh\u1eafn";
        };
    }

    private String preview(ChatMessageResponse message) {
        if (message.content() != null && !message.content().isBlank()) {
            String content = message.content().trim();
            return content.length() > 120 ? content.substring(0, 117) + "..." : content;
        }
        return switch (message.messageType()) {
            case IMAGE -> "Đã gửi một ảnh";
            case VIDEO -> "Đã gửi một video";
            case AUDIO -> "Đã gửi một tin nhắn thoại";
            case FILE -> "Đã gửi một tệp";
            default -> "Đã gửi một tin nhắn";
        };
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
    }
}
