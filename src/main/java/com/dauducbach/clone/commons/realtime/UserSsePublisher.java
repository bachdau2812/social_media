package com.dauducbach.clone.commons.realtime;

import reactor.core.publisher.Mono;

public interface UserSsePublisher {
    Mono<Void> sendToUser(String userId, String event, String data);
}
