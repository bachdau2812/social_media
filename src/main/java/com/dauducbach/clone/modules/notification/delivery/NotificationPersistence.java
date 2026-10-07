package com.dauducbach.clone.modules.notification.delivery;

import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import com.dauducbach.clone.modules.notification.entity.UserNotifications;
import reactor.core.publisher.Mono;

public interface NotificationPersistence {
    Mono<Boolean> persistIfNew(NotificationEvents event, UserNotifications recipient);
}
