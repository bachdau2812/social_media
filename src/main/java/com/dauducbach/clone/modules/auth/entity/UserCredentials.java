package com.dauducbach.clone.modules.auth.entity;

import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Table("user_credentials")
public class UserCredentials {
    @Id
    String userId;
    String username;
    String userPassword;
    String userRole;
    String email;
    String provider;
    String providerId;
}
