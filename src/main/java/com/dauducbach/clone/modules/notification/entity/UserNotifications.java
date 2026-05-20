package com.dauducbach.clone.modules.notification.entity;

import com.dauducbach.clone.modules.notification.constants.NotificationStatus;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder

public class UserNotifications {
    @Id
    String id;
    String userId;
    String eventId;
    NotificationStatus notificationStatus;
    Instant readAt;
    Instant createdAt;
}
