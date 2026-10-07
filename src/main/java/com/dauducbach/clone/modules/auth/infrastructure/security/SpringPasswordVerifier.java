package com.dauducbach.clone.modules.auth.infrastructure.security;

import com.dauducbach.clone.modules.auth.credentials.PasswordVerifier;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SpringPasswordVerifier implements PasswordVerifier {
    private final PasswordEncoder passwordEncoder;

    @Override
    public boolean matches(String rawPassword, String passwordHash) {
        return passwordEncoder.matches(rawPassword, passwordHash);
    }
}
