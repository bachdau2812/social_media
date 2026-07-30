package com.dauducbach.clone.commons.security;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;

public final class ActorIdentity {
    private ActorIdentity() {
    }

    public static String require(String authenticatedUserId, String requestedActorId) {
        String principal = normalize(authenticatedUserId);
        if (principal.isEmpty()) {
            throw new AppException(ErrorCode.AUTHENTICATION_FAILED, "Authenticated user identity is required");
        }

        String requested = normalize(requestedActorId);
        if (!requested.isEmpty() && !principal.equals(requested)) {
            throw new AppException(ErrorCode.AUTHENTICATION_FAILED, "Requested actor does not match authenticated user");
        }
        return principal;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }
}
