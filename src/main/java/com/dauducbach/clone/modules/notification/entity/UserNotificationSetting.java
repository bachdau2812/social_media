package com.dauducbach.clone.modules.notification.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Table("user_setting_notify")
public class UserNotificationSetting {
    @Id
    String userId;
    boolean pushNotification;
    boolean emailNotification;
    boolean likeMyPost;
    boolean likeFriendPost;
    boolean commentMyPost;
    boolean commentFriendPost;
    boolean newMessage;
}
