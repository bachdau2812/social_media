package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.dto.request.ChatSocketClientFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

@Component
@RequiredArgsConstructor
public class ChatWebSocketHandler implements WebSocketHandler {
    private static final Logger log = LoggerFactory.getLogger(ChatWebSocketHandler.class);
    private static final Duration HEARTBEAT_TIMEOUT = Duration.ofSeconds(75);

    private final ObjectMapper objectMapper;
    private final ChatSessionRegistry sessionRegistry;
    private final ChatPresenceService presenceService;
    private final ChatCursorService cursorService;

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.getHandshakeInfo().getPrincipal()
                .switchIfEmpty(Mono.error(new IllegalStateException("Authenticated WebSocket principal is required")))
                .flatMap(principal -> openSession(session, principal.getName()));
    }

    private Mono<Void> openSession(WebSocketSession session, String userId) {
        String sessionId = session.getId();
        ChatSessionRegistry.SessionState state = sessionRegistry.register(userId, sessionId);

        Mono<Void> inbound = session.receive()
                .concatMap(message -> handleFrame(userId, sessionId, message.getPayloadAsText()))
                .then();
        Mono<Void> outbound = session.send(state.outbound().asFlux().map(session::textMessage));
        Mono<Void> watchdog = Flux.interval(Duration.ofSeconds(15))
                .filter(tick -> sessionRegistry.isStale(
                        userId,
                        sessionId,
                        Instant.now().minus(HEARTBEAT_TIMEOUT)))
                .next()
                .flatMap(tick -> session.close(CloseStatus.GOING_AWAY));

        return presenceService.refresh(userId, sessionId)
                .then(cursorService.markPendingDeliveredOnConnect(userId))
                .then(Mono.firstWithSignal(inbound, outbound, watchdog))
                .doFinally(signal -> {
                    sessionRegistry.remove(userId, sessionId);
                    presenceService.remove(userId, sessionId)
                            .onErrorResume(error -> Mono.empty())
                            .subscribe();
                });
    }

    private Mono<Void> handleFrame(String userId, String sessionId, String payload) {
        try {
            ChatSocketClientFrame frame = objectMapper.readValue(payload, ChatSocketClientFrame.class);
            String type = frame.type() == null ? "" : frame.type().trim().toUpperCase();
            if ("HEARTBEAT".equals(type)) {
                sessionRegistry.heartbeat(userId, sessionId);
                return presenceService.refresh(userId, sessionId);
            }
            if ("DELIVERED_ACK".equals(type) && frame.sequence() != null) {
                return cursorService.markDelivered(userId, frame.conversationId(), frame.sequence()).then();
            }
            if ("READ_ACK".equals(type) && frame.sequence() != null) {
                return cursorService.markRead(userId, frame.conversationId(), frame.sequence()).then();
            }
            return Mono.empty();
        } catch (Exception error) {
            log.warn("|ChatWebSocketHandler|handleFrame|invalid frame|userId={}|error={}", userId, error.getMessage());
            return Mono.empty();
        }
    }
}
