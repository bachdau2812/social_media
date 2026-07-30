package com.dauducbach.clone.modules.chat.service;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChatPresenceServiceTest {

    @Test
    void refreshRegistersInstanceScopedSession() {
        Fixture fixture = new Fixture();
        when(fixture.zSet.add(eq("chat:presence:user-sessions:user-1"),
                eq("node-a:session-1"), anyDouble())).thenReturn(Mono.just(true));
        when(fixture.template.expire("chat:presence:user-sessions:user-1", Duration.ofSeconds(180)))
                .thenReturn(Mono.just(true));
        when(fixture.values.set("chat:presence:session:user-1:node-a:session-1",
                "online", Duration.ofSeconds(90))).thenReturn(Mono.just(true));
        when(fixture.values.set(eq("chat:presence:last-active:user-1"), anyString()))
                .thenReturn(Mono.just(true));

        StepVerifier.create(fixture.service.refresh("user-1", "session-1"))
                .verifyComplete();

        verify(fixture.zSet).add(eq("chat:presence:user-sessions:user-1"),
                eq("node-a:session-1"), anyDouble());
    }

    @Test
    void removingLocalSessionKeepsUserOnlineWhenAnotherInstanceSessionExists() {
        Fixture fixture = new Fixture();
        when(fixture.template.delete("chat:presence:session:user-1:node-a:session-1"))
                .thenReturn(Mono.just(1L));
        when(fixture.zSet.remove("chat:presence:user-sessions:user-1", "node-a:session-1"))
                .thenReturn(Mono.just(1L));
        when(fixture.zSet.range(eq("chat:presence:user-sessions:user-1"),
                org.mockito.ArgumentMatchers.<Range<Long>>any()))
                .thenReturn(Flux.just("node-b:session-9"));
        when(fixture.template.hasKey("chat:presence:session:user-1:node-b:session-9"))
                .thenReturn(Mono.just(true));

        StepVerifier.create(fixture.service.remove("user-1", "session-1"))
                .verifyComplete();

        verify(fixture.values, never()).set(
                eq("chat:presence:last-active:user-1"), anyString());
    }

    private static final class Fixture {
        private final ReactiveRedisTemplate<String, Object> template = mock(ReactiveRedisTemplate.class);
        private final ReactiveZSetOperations<String, Object> zSet = mock(ReactiveZSetOperations.class);
        private final ReactiveValueOperations<String, Object> values = mock(ReactiveValueOperations.class);
        private final ChatPresenceService service = new ChatPresenceService(template, "node-a");

        private Fixture() {
            when(template.opsForZSet()).thenReturn(zSet);
            when(template.opsForValue()).thenReturn(values);
        }
    }
}
