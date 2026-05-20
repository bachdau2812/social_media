package com.dauducbach.clone.modules.notification.entity;

import com.dauducbach.clone.commons.constant.UserActionType;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Data
@AllArgsConstructor
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder

@Table("notification_template")
public class NotificationTemplates {
    @Id
    int id;
    UserActionType actionType;
    String template;
}
