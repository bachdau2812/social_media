package com.dauducbach.clone.modules.notification.dto;

import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.commons.constant.UserActionType;
import lombok.*;
import lombok.experimental.FieldDefaults;

@Data
@AllArgsConstructor
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder

public class NotificationForService {
    String actorId;
    UserActionType actionType;
    String entityId;
    String entityType;
    String recipient;
    String title;
    String htmlContent;
    NotificationType notificationType;
}
