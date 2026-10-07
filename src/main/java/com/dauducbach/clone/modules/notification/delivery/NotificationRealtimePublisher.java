package com.dauducbach.clone.modules.notification.delivery;

import reactor.core.publisher.Mono;

public interface NotificationRealtimePublisher {
    Mono<Void> notifyChanged(String userId, String notificationId);
}
