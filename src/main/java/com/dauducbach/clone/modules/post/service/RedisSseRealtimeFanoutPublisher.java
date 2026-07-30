package com.dauducbach.clone.modules.post.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class RedisSseRealtimeFanoutPublisher implements SseRealtimeFanoutPublisher {
    private final ReactiveStringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> publish(String userId, String event, String data) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(
                        new SseFanoutMessage(userId, event, data)))
                .flatMap(payload -> redisTemplate.convertAndSend(CHANNEL, payload))
                .then();
    }
}