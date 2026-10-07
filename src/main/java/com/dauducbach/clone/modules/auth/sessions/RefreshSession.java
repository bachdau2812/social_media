package com.dauducbach.clone.modules.auth.sessions;

import java.time.Instant;

public record RefreshSession(
        String id,
        String userId,
        String tokenHash,
        Instant expiresAt,
        Instant createdAt,
        String deviceInfo) {
}
