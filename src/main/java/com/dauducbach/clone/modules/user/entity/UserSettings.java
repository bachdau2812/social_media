package com.dauducbach.clone.modules.user.entity;

import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)
@Table("user_settings")
public class UserSettings {
    @Id
    String userId;
    String accountVisibility;
    String storyVisibility;
    String commentPermission;
    String mentionPermission;
    boolean tagApprovalRequired;
    boolean activityStatusVisible;
    boolean readReceiptsEnabled;
    boolean pushEnabled;
    boolean emailEnabled;
    boolean likesEnabled;
    boolean commentsEnabled;
    boolean followsEnabled;
    boolean mentionsEnabled;
    boolean storiesEnabled;
    boolean messagesEnabled;
    boolean securityEnabled;
    String sensitiveContentLevel;
    String autoplayVideo;
    String theme;
    boolean reducedMotion;
    double textScale;
    boolean highContrast;
    boolean alwaysShowCaptions;
    Instant updatedAt;

    public static UserSettings defaults(String userId) {
        return UserSettings.builder()
                .userId(userId)
                .accountVisibility("PUBLIC")
                .storyVisibility("FOLLOWERS")
                .commentPermission("EVERYONE")
                .mentionPermission("EVERYONE")
                .tagApprovalRequired(false)
                .activityStatusVisible(true)
                .readReceiptsEnabled(true)
                .pushEnabled(true)
                .emailEnabled(false)
                .likesEnabled(true)
                .commentsEnabled(true)
                .followsEnabled(true)
                .mentionsEnabled(true)
                .storiesEnabled(true)
                .messagesEnabled(true)
                .securityEnabled(true)
                .sensitiveContentLevel("STANDARD")
                .autoplayVideo("WIFI_ONLY")
                .theme("SYSTEM")
                .reducedMotion(false)
                .textScale(1.0)
                .highContrast(false)
                .alwaysShowCaptions(false)
                .updatedAt(Instant.now())
                .build();
    }
}