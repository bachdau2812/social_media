package com.dauducbach.clone.modules.chat.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class RedisChatRealtimeFanoutPublisher implements ChatRealtimeFanoutPublisher {
    private final ReactiveStringRedisTemplate redisTemplate;

    @Override
    public Mono<Void> publish(String payload) {
        if (payload == null || payload.isBlank()) {
            return Mono.empty();
        }
        return redisTemplate.convertAndSend(CHANNEL, payload).then();
    }
}