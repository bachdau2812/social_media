package com.dauducbach.clone.modules.auth.credentials;

public interface PasswordVerifier {
    boolean matches(String rawPassword, String passwordHash);
}
