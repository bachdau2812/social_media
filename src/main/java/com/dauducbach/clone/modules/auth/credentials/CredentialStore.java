package com.dauducbach.clone.modules.auth.credentials;

import reactor.core.publisher.Mono;

public interface CredentialStore {
    Mono<CredentialAccount> findByUsername(String username);

    Mono<CredentialAccount> findById(String userId);
}
