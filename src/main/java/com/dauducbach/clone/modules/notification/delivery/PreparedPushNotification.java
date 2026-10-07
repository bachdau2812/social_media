package com.dauducbach.clone.modules.notification.delivery;

import com.dauducbach.clone.modules.notification.entity.NotificationEvents;
import com.dauducbach.clone.modules.notification.entity.UserNotifications;

record PreparedPushNotification(
        NotificationEvents event,
        UserNotifications userNotification,
        String deepLink,
        String dedupKey
) {
}
