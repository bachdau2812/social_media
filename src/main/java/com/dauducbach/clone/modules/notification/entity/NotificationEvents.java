package com.dauducbach.clone.modules.notification.entity;

import com.dauducbach.clone.commons.constant.UserActionType;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder

@Table("notification_events")
public class NotificationEvents {
    @Id
    String id;
    String actorId;     // ID của người thực hiện hành động
    UserActionType actionType;  // ID của loại hành động (ví dụ: "like", "comment", "follow")
    String entityId;    // ID của đối tượng bị tác động (ví dụ: bài viet, bình luận, người dùng)
    String entityType;  // Loại đối tượng bị tác động (ví dụ: "post", "comment", "user")
    String content;
    String metadata;
    String deepLink;
    String dedupKey;
    Instant createdAt;
}
