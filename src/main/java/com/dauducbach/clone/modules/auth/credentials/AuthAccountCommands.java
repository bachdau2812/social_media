package com.dauducbach.clone.modules.auth.credentials;

import com.dauducbach.clone.modules.auth.registration.RegistrationDraft;
import reactor.core.publisher.Mono;

public interface AuthAccountCommands {
    Mono<Boolean> usernameExists(String username);

    Mono<Boolean> emailExists(String email);

    Mono<String> register(RegistrationDraft draft);

    Mono<CredentialAccount> findByEmail(String email);

    Mono<Void> updatePassword(String userId, String rawPassword);

    Mono<Void> updateUsernameAndPassword(String userId, String username, String rawPassword);
}
