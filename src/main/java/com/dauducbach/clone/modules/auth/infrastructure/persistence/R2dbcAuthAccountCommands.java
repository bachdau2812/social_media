package com.dauducbach.clone.modules.auth.infrastructure.persistence;

import com.dauducbach.clone.modules.auth.credentials.AuthAccountCommands;
import com.dauducbach.clone.modules.auth.credentials.CredentialAccount;
import com.dauducbach.clone.modules.auth.entity.UserCredentials;
import com.dauducbach.clone.modules.auth.registration.RegistrationDraft;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class R2dbcAuthAccountCommands implements AuthAccountCommands {
    private final R2dbcEntityTemplate entityTemplate;
    private final UserCredentialsRepository credentialsRepository;
    private final PasswordEncoder passwordEncoder;

    @Override
    public Mono<Boolean> usernameExists(String username) {
        return credentialsRepository.existsByUsername(username);
    }

    @Override
    public Mono<Boolean> emailExists(String email) {
        return credentialsRepository.existsByEmail(email);
    }

    @Override
    public Mono<String> register(RegistrationDraft draft) {
        UserCredentials account = UserCredentials.builder()
                .userId(UUID.randomUUID().toString())
                .username(draft.username())
                .email(draft.email())
                .userPassword(passwordEncoder.encode(draft.password()))
                .userRole(draft.role())
                .build();
        return entityTemplate.insert(UserCredentials.class)
                .using(account)
                .map(UserCredentials::getUserId);
    }

    @Override
    public Mono<CredentialAccount> findByEmail(String email) {
        return credentialsRepository.findByEmail(email)
                .map(account -> new CredentialAccount(
                        account.getUserId(), account.getUsername(), account.getUserPassword(), account.getUserRole()));
    }

    @Override
    public Mono<Void> updatePassword(String userId, String rawPassword) {
        return credentialsRepository.findById(userId)
                .flatMap(account -> {
                    account.setUserPassword(passwordEncoder.encode(rawPassword));
                    return credentialsRepository.save(account);
                })
                .then();
    }

    @Override
    public Mono<Void> updateUsernameAndPassword(String userId, String username, String rawPassword) {
        return credentialsRepository.findById(userId)
                .flatMap(account -> {
                    account.setUsername(username);
                    account.setUserPassword(passwordEncoder.encode(rawPassword));
                    return credentialsRepository.save(account);
                })
                .then();
    }
}
