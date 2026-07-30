package com.dauducbach.clone.modules.chat.service;

import org.springframework.stereotype.Component;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ChatSessionRegistry {
    private final Map<String, Map<String, SessionState>> sessions = new ConcurrentHashMap<>();

    public SessionState register(String userId, String sessionId) {
        SessionState state = new SessionState(
                Sinks.many().unicast().onBackpressureBuffer(),
                Instant.now());
        sessions.computeIfAbsent(userId, ignored -> new ConcurrentHashMap<>())
                .put(sessionId, state);
        return state;
    }

    public void heartbeat(String userId, String sessionId) {
        SessionState state = get(userId, sessionId);
        if (state != null) {
            state.lastHeartbeat = Instant.now();
        }
    }

    public boolean isStale(String userId, String sessionId, Instant threshold) {
        SessionState state = get(userId, sessionId);
        return state == null || state.lastHeartbeat.isBefore(threshold);
    }

    public void sendToUser(String userId, String payload) {
        Map<String, SessionState> userSessions = sessions.get(userId);
        if (userSessions == null) {
            return;
        }
        userSessions.values().forEach(state -> state.outbound.tryEmitNext(payload));
    }

    public boolean remove(String userId, String sessionId) {
        Map<String, SessionState> userSessions = sessions.get(userId);
        if (userSessions == null) {
            return false;
        }
        SessionState removed = userSessions.remove(sessionId);
        if (removed != null) {
            removed.outbound.tryEmitComplete();
        }
        if (userSessions.isEmpty()) {
            sessions.remove(userId, userSessions);
            return false;
        }
        return true;
    }

    private SessionState get(String userId, String sessionId) {
        Map<String, SessionState> userSessions = sessions.get(userId);
        return userSessions == null ? null : userSessions.get(sessionId);
    }

    public static final class SessionState {
        private final Sinks.Many<String> outbound;
        private volatile Instant lastHeartbeat;

        private SessionState(Sinks.Many<String> outbound, Instant lastHeartbeat) {
            this.outbound = outbound;
            this.lastHeartbeat = lastHeartbeat;
        }

        public Sinks.Many<String> outbound() {
            return outbound;
        }
    }
}
