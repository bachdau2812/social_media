package com.dauducbach.clone.modules.user.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder
@Table("user_stories")
public class UserStories {
    @Id
    String id;
    String userId;
    String mediaUrl;
    String mediaType;
    String musicUrl;
    Long musicStart;
    Long musicEnd;
    String status;
    Instant createdAt;
    Instant expiredAt;
}
