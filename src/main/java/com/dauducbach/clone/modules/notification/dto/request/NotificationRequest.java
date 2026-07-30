package com.dauducbach.clone.modules.notification.dto.request;

import com.dauducbach.clone.modules.notification.constants.NotificationType;
import com.dauducbach.clone.commons.constant.UserActionType;
import lombok.*;
import lombok.experimental.FieldDefaults;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder

public class NotificationRequest {
    String actorId;
    UserActionType actionType;
    String entityId;
    String entityType;
    List<String> recipientIds;
    String title;
    String content;
    Map<String, String> metadata;
    String deepLink;
    String dedupKey;
    NotificationType notificationType;
}
