package com.dauducbach.clone.modules.auth.credentials;

public interface TemporarySecretGenerator {
    String verificationCode(int length);

    String password(int length);
}
