package com.dauducbach.clone.modules.chat.service;

import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class ChatSessionRegistryTest {
    @Test
    void closesSlowSessionInsteadOfGrowingAnUnboundedOutboundQueue() {
        ChatSessionRegistry registry = new ChatSessionRegistry();
        ChatSessionRegistry.SessionState session = registry.register("user-1", "session-1");

        for (int index = 0; index < 257; index++) {
            registry.sendToUser("user-1", "event-" + index);
        }

        StepVerifier.create(session.outbound().asFlux())
                .expectNext("event-0")
                .expectNextCount(255)
                .expectErrorMatches(error -> error.getMessage().contains("outbound queue is full"))
                .verify();
        assertThat(registry.remove("user-1", "session-1")).isFalse();
    }

    @Test
    void closesSessionsSoClientsReconnectAndRecoverAfterFanoutRestarts() {
        ChatSessionRegistry registry = new ChatSessionRegistry();
        ChatSessionRegistry.SessionState session = registry.register("user-1", "session-1");

        registry.closeAll(new IllegalStateException("Redis subscriber lost"));

        StepVerifier.create(session.outbound().asFlux())
                .expectErrorMatches(error -> error.getMessage().contains("reconnect to synchronize"))
                .verify();
    }
}
