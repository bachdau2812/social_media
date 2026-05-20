package com.dauducbach.clone.infrastructure.dto;

import com.dauducbach.clone.modules.notification.constants.TargetOfActionType;
import com.dauducbach.clone.commons.constant.UserActionType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Table("user_activities")
public class UserActivities {
    @Id
    String id;
    String userId;
    UserActionType actionType;

    String targetId;
    TargetOfActionType targetType;

    List<String> metadata;

    String status;
    String ipAddress;
    Instant createdAt;
}
