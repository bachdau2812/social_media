package com.dauducbach.clone.modules.chat.service;

import reactor.core.publisher.Mono;

public interface ChatRealtimeFanoutPublisher {
    String CHANNEL = "chat:realtime:fanout:v1";

    Mono<Void> publish(String payload);
}