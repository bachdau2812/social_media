package com.dauducbach.clone.commons.object_cache;

import lombok.*;
import lombok.experimental.FieldDefaults;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder

public class UserInformation {
    String userId;
    String username;
    String email;
    String userRole;
    String provider;
    String providerId;
}
