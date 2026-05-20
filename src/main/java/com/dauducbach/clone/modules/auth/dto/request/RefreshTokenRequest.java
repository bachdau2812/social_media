package com.dauducbach.clone.modules.auth.dto.request;

import lombok.*;
import lombok.experimental.FieldDefaults;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder

public class RefreshTokenRequest {
    String refreshToken;
    String deviceInfo;
}
