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

@Table("user_follower")
public class UserFollower {
    @Id
    String id;
    String followerId;    // Người thực hiện follow
    String followingId;   // Người được follow
    Instant createdAt;     // Thời gian follow

    // Constructor cho dễ builder
    public static UserFollower create(String followerId, String followingId) {
        return UserFollower.builder()
                .id(java.util.UUID.randomUUID().toString())
                .followerId(followerId)
                .followingId(followingId)
                .createdAt(Instant.now())
                .build();
    }
}