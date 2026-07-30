package com.dauducbach.clone.modules.post.service;

import reactor.core.publisher.Mono;

public interface SseRealtimeFanoutPublisher {
    String CHANNEL = "sse:realtime:fanout:v1";

    Mono<Void> publish(String userId, String event, String data);
}