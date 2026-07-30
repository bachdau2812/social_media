package com.dauducbach.clone.modules.notification.service;

import reactor.core.publisher.Mono;

public interface NotificationPushGateway {
    Mono<String> send(NotificationPushPayload payload);
}
