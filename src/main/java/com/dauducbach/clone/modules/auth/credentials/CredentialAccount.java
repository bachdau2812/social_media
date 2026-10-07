package com.dauducbach.clone.modules.auth.credentials;

public record CredentialAccount(
        String userId,
        String username,
        String passwordHash,
        String role) {
}
