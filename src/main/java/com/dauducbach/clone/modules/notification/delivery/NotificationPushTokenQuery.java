package com.dauducbach.clone.modules.notification.delivery;

import reactor.core.publisher.Mono;

public interface NotificationPushTokenQuery {
    Mono<String> findDeviceToken(String userId);
}
