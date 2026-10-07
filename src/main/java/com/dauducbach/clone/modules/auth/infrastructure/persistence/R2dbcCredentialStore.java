package com.dauducbach.clone.modules.auth.infrastructure.persistence;

import com.dauducbach.clone.modules.auth.credentials.CredentialAccount;
import com.dauducbach.clone.modules.auth.credentials.CredentialStore;
import com.dauducbach.clone.modules.auth.entity.UserCredentials;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class R2dbcCredentialStore implements CredentialStore {
    private final UserCredentialsRepository repository;

    @Override
    public Mono<CredentialAccount> findByUsername(String username) {
        return repository.findByUsername(username).map(R2dbcCredentialStore::toAccount);
    }

    @Override
    public Mono<CredentialAccount> findById(String userId) {
        return repository.findById(userId).map(R2dbcCredentialStore::toAccount);
    }

    private static CredentialAccount toAccount(UserCredentials credentials) {
        return new CredentialAccount(
                credentials.getUserId(),
                credentials.getUsername(),
                credentials.getUserPassword(),
                credentials.getUserRole());
    }
}
