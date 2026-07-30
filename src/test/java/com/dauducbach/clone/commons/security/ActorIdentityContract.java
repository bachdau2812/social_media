package com.dauducbach.clone.commons.security;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;

public final class ActorIdentityContract {
    private ActorIdentityContract() {
    }

    public static void main(String[] args) {
        String principal = "user-1";
        if (!principal.equals(ActorIdentity.require(principal, principal))) {
            throw new AssertionError("Matching actor must resolve to the authenticated principal");
        }
        if (!principal.equals(ActorIdentity.require(principal, " "))) {
            throw new AssertionError("Blank compatibility actor must resolve to the authenticated principal");
        }

        assertAuthenticationFailure(() -> ActorIdentity.require(principal, "user-2"));
        assertAuthenticationFailure(() -> ActorIdentity.require(" ", principal));
    }

    private static void assertAuthenticationFailure(Runnable operation) {
        try {
            operation.run();
            throw new AssertionError("Expected actor identity validation to fail");
        } catch (AppException exception) {
            if (exception.getErrorCode() != ErrorCode.AUTHENTICATION_FAILED) {
                throw new AssertionError("Unexpected error code: " + exception.getErrorCode());
            }
        }
    }
}
