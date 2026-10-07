package com.dauducbach.clone.modules.auth.sessions;

public record AuthenticatedSession(
        String accessToken,
        String refreshToken,
        String deviceInfo,
        String userId,
        String username) {
}
