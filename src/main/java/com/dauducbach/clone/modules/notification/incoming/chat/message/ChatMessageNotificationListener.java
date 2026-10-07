package com.dauducbach.clone.modules.notification.incoming.chat.message;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.chat.publicapi.ChatEventType;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.publicapi.ChatEventTopics;
import com.dauducbach.clone.modules.chat.publicapi.ChatNotificationQuery;
import com.dauducbach.clone.modules.chat.publicapi.ChatNotificationConversation;
import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import com.dauducbach.clone.modules.notification.repository.NotificationTemplatesRepository;
import com.dauducbach.clone.modules.notification.service.PushNotificationService;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
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
    private final ChatNotificationQuery chatNotificationQueryService;
    private final UserIdentityQuery userIdentityQuery;

    @KafkaListener(
            topics = ChatEventTopics.MESSAGE_CREATED,
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

            // Resolve once per event, and only when a recipient is eligible for a notification.
            Mono<MessageNotificationContext> context = Mono.defer(() -> Mono.zip(
                            chatNotificationQueryService.findConversation(event.conversationId()),
                            senderName(event.message()))
                    .map(tuple -> new MessageNotificationContext(tuple.getT1(), tuple.getT2())))
                    .cache();

            return Flux.fromIterable(event.recipientIds())
                    .filter(recipientId -> !recipientId.equals(event.actorId()))
                    .concatMap(recipientId -> sendPush(recipientId, event, context))
                    .then()
                    .doOnError(error -> log.error(
                            "|ChatMessageNotificationListener|handle|failed|conversationId={}|error={}",
                            event.conversationId(), error.getMessage()))
                    .toFuture();
        } catch (Exception error) {
            log.error("|ChatMessageNotificationListener|handle|invalid payload|key={}|error={}",
                    record.key(), error.getMessage());
            return CompletableFuture.completedFuture(null);
        }
    }

    private Mono<Void> sendPush(String recipientId, ChatEvent event, Mono<MessageNotificationContext> context) {
        return chatNotificationQueryService.canReceiveMessageNotification(event.conversationId(), recipientId, Instant.now())
                .filter(Boolean::booleanValue)
                .flatMap(ignored -> templateRepository.findByActionType(UserActionType.SEND_MESSAGE)
                        .map(template -> template.getTemplate())
                        .defaultIfEmpty("{USERNAME}: {MESSAGE}")
                        .flatMap(template -> context.flatMap(display -> {
                            ChatMessageResponse message = event.message();
                            String sender = display.sender();
                            String preview = previewEnhanced(message);
                            String body = display.conversation().group()
                                    ? sender + " đã gửi một tin nhắn: \"" + preview + "\""
                                    : notificationBody(sender, message, preview);
                            Map<String, String> metadata = new HashMap<>();
                            metadata.put("EVENT_ID", event.eventId());
                            metadata.put("CONVERSATION_ID", event.conversationId());
                            metadata.put("MESSAGE_ID", message.id());
                            metadata.put("MESSAGE_SEQ", String.valueOf(message.messageSeq()));
                            metadata.put("MESSAGE_TYPE", message.messageType().name());
                            metadata.put("MESSAGE_PREVIEW", preview);
                            if (display.conversation().group()) {
                                metadata.put("GROUP_NAME", display.conversation().title());
                            }
                            return pushNotificationService.sendPushNotification(
                                    NotificationForService.builder()
                                            .actorId(event.actorId())
                                            .actionType(UserActionType.SEND_MESSAGE)
                                            .entityId(message.id())
                                            .entityType("CHAT_MESSAGE")
                                            .recipient(recipientId)
                                            .title(display.conversation().group() ? display.conversation().title() : sender)
                                            .htmlContent(body)
                                            .metadata(metadata)
                                            .notificationType(NotificationType.PUSH)
                                            .build());
                        })))
                .then();
    }

    private Mono<String> senderName(ChatMessageResponse message) {
        String eventName = message.senderDisplayName();
        if (eventName != null && !eventName.isBlank() && !eventName.trim().equals(message.senderId())) {
            return Mono.just(eventName.trim());
        }
        return userIdentityQuery.resolveDisplayName(message.senderId())
                .filter(name -> !name.isBlank() && !name.trim().equals(message.senderId()))
                .map(String::trim)
                .defaultIfEmpty("Người dùng");
    }

    private record MessageNotificationContext(ChatNotificationConversation conversation, String sender) {
    }


    private String notificationBody(String sender, ChatMessageResponse message, String preview) {
        if (message.messageType() == MessageType.STORY_REPLY) {
            return sender + " đã trả lời tin của bạn: " + (char) 34 + preview + (char) 34;
        }
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
