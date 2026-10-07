package com.dauducbach.clone.modules.media.music.fetch;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class FetchMusicWaiters {
    private final ConcurrentHashMap<String, NotificationState> states = new ConcurrentHashMap<>();

    public Registration register(String trackId, String userId) {
        AtomicReference<Registration> registration = new AtomicReference<>();
        states.compute(trackId, (ignored, current) -> {
            NotificationState state = current == null ? new NotificationState() : current;
            registration.set(state.register(userId));
            return state;
        });
        return registration.get();
    }

    public NotificationState state(String trackId) {
        return states.computeIfAbsent(trackId, ignored -> new NotificationState());
    }

    public void unregister(String trackId, Registration registration) {
        if (registration == null || !registration.release()) {
            return;
        }
        states.compute(trackId, (ignored, current) -> {
            if (current != registration.state()) {
                return current;
            }
            return current.unregisterAndIsEmpty(registration.userId()) ? null : current;
        });
    }

    public void retire(String trackId, NotificationState state) {
        states.compute(trackId, (ignored, current) -> current == state ? null : current);
    }

    public record Terminal(String event, String payload) {
    }

    public static final class Registration {
        private final NotificationState state;
        private final String userId;
        private final Terminal terminal;
        private final AtomicBoolean active;

        private Registration(NotificationState state, String userId, Terminal terminal) {
            this.state = state;
            this.userId = userId;
            this.terminal = terminal;
            this.active = new AtomicBoolean(terminal == null);
        }

        public NotificationState state() {
            return state;
        }

        public String userId() {
            return userId;
        }

        public Terminal terminal() {
            return terminal;
        }

        private boolean release() {
            return active.compareAndSet(true, false);
        }
    }

    public static final class NotificationState {
        private final Map<String, Integer> waiterCounts = new HashMap<>();
        private Terminal terminal;

        private synchronized Registration register(String userId) {
            if (terminal == null) {
                waiterCounts.merge(userId, 1, Integer::sum);
            }
            return new Registration(this, userId, terminal);
        }

        private synchronized boolean unregisterAndIsEmpty(String userId) {
            Integer count = waiterCounts.get(userId);
            if (count != null) {
                if (count <= 1) {
                    waiterCounts.remove(userId);
                } else {
                    waiterCounts.put(userId, count - 1);
                }
            }
            return terminal == null && waiterCounts.isEmpty();
        }

        public synchronized Set<String> complete(Terminal result) {
            if (terminal == null) {
                terminal = result;
            }
            Set<String> snapshot = Set.copyOf(waiterCounts.keySet());
            waiterCounts.clear();
            return snapshot;
        }
    }
}
