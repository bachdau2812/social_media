package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class KafkaChatEventPublisher implements ChatEventPublisher {
    private static final Logger log = LoggerFactory.getLogger(KafkaChatEventPublisher.class);
    public static final String MESSAGE_CREATED_TOPIC = "chat.message.created";
    public static final String CURSOR_UPDATED_TOPIC = "chat.cursor.updated";
    public static final String MEMBER_REQUESTED_TOPIC = "chat.member.requested";
    public static final String MEMBERSHIP_CHANGED_TOPIC = "chat.membership.changed";

    KafkaSender<String, String> kafkaSender;
    ObjectMapper objectMapper;

    @Override
    public Mono<Void> publish(ChatEvent event) {
        if (event == null || event.conversationId() == null || event.conversationId().isBlank()) {
            return Mono.error(new AppException(ErrorCode.CHAT_EVENT_PUBLISH_FAILED, "Chat event is invalid"));
        }

        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException error) {
            return Mono.error(new AppException(ErrorCode.CHAT_EVENT_PUBLISH_FAILED, "Serialize chat event failed", error));
        }
        String topic = switch (event.type()) {
            case CURSOR_UPDATED -> CURSOR_UPDATED_TOPIC;
            case MEMBER_REQUESTED -> MEMBER_REQUESTED_TOPIC;
            case GROUP_CREATED, MEMBER_ADDED, MEMBER_REMOVED -> MEMBERSHIP_CHANGED_TOPIC;
            default -> MESSAGE_CREATED_TOPIC;
        };
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(topic, event.conversationId(), payload),
                event.eventId());

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|KafkaChatEventPublisher|publish|failed|conversationId={}|error={}",
                        event.conversationId(), error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CHAT_EVENT_PUBLISH_FAILED, "Publish chat event failed", error))
                .then();
    }
}
