package com.dauducbach.clone.modules.auth.infrastructure.security;

import com.dauducbach.clone.modules.auth.credentials.TemporarySecretGenerator;
import org.springframework.stereotype.Component;

import java.security.SecureRandom;

@Component
public class SecureTemporarySecretGenerator implements TemporarySecretGenerator {
    private static final String CODE_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final String PASSWORD_CHARACTERS = CODE_CHARACTERS + "!@#$%^&*()-_=+[]{};:,.<>?/";
    private final SecureRandom random = new SecureRandom();

    @Override
    public String verificationCode(int length) {
        return generate(length, CODE_CHARACTERS);
    }

    @Override
    public String password(int length) {
        return generate(length, PASSWORD_CHARACTERS);
    }

    private String generate(int length, String alphabet) {
        StringBuilder result = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            result.append(alphabet.charAt(random.nextInt(alphabet.length())));
        }
        return result.toString();
    }
}
