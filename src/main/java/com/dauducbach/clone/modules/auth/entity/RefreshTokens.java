package com.dauducbach.clone.modules.auth.entity;

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

@Table("refresh_tokens")
public class RefreshTokens {
    @Id
    String id;
    String userId;
    String tokenHash;
    Instant expiredTime;
    Instant createdAt;
    Instant updatedAt;
    String deviceInfo;
    boolean revoked;
}
