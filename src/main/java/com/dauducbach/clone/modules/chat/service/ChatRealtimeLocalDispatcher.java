package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class ChatRealtimeLocalDispatcher {
    private static final Logger log = LoggerFactory.getLogger(ChatRealtimeLocalDispatcher.class);

    private final ObjectMapper objectMapper;
    private final ChatSessionRegistry sessionRegistry;

    public Mono<Void> dispatch(String payload) {
        return Mono.fromRunnable(() -> {
            try {
                ChatEvent event = objectMapper.readValue(payload, ChatEvent.class);
                event.recipientIds().stream()
                        .filter(recipientId -> recipientId != null && !recipientId.isBlank())
                        .distinct()
                        .forEach(recipientId -> sessionRegistry.sendToUser(recipientId, payload));
            } catch (Exception error) {
                throw new IllegalArgumentException("Invalid Chat realtime payload", error);
            }
        }).doOnError(error -> log.error(
                "|ChatRealtimeLocalDispatcher|dispatch|failed|error={}",
                error.getMessage())).then();
    }
}