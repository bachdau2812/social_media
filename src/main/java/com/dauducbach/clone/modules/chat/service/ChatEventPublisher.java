package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import reactor.core.publisher.Mono;

public interface ChatEventPublisher {

    Mono<Void> publish(ChatEvent event);
}
