package com.dauducbach.clone.modules.notification.delivery;

import reactor.core.publisher.Mono;

public interface NotificationPushGateway {
    Mono<String> send(NotificationPushPayload payload);
}
