package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class ChatRealtimeEventListener {
    private static final Logger log = LoggerFactory.getLogger(ChatRealtimeEventListener.class);

    private final ObjectMapper objectMapper;
    private final ChatRealtimeFanoutPublisher fanoutPublisher;

    @KafkaListener(
            topics = {
                    KafkaChatEventPublisher.MESSAGE_CREATED_TOPIC,
                    KafkaChatEventPublisher.CURSOR_UPDATED_TOPIC,
                    KafkaChatEventPublisher.MEMBERSHIP_CHANGED_TOPIC
            },
            groupId = "chat-realtime-service")
    public CompletableFuture<Void> handle(ConsumerRecord<String, String> record) {
        try {
            ChatEvent event = objectMapper.readValue(record.value(), ChatEvent.class);
            if (event.conversationId() == null
                    || !event.conversationId().equals(record.key())) {
                log.warn(
                        "|ChatRealtimeEventListener|handle|invalid key|key={}|conversationId={}",
                        record.key(), event.conversationId());
                return CompletableFuture.completedFuture(null);
            }
            return fanoutPublisher.publish(record.value())
                    .doOnError(error -> log.error(
                            "|ChatRealtimeEventListener|handle|fanout failed|key={}|error={}",
                            record.key(), error.getMessage()))
                    .toFuture();
        } catch (Exception error) {
            log.error(
                    "|ChatRealtimeEventListener|handle|failed|key={}|error={}",
                    record.key(), error.getMessage());
            return CompletableFuture.failedFuture(error);
        }
    }
}