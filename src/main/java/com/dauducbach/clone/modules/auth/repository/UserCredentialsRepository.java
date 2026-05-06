package com.dauducbach.clone.modules.auth.repository;

import com.dauducbach.clone.modules.auth.entity.UserCredentials;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface UserCredentialsRepository extends ReactiveCrudRepository<UserCredentials, String> {
    Mono<UserCredentials> findByEmail(String email);

    Mono<Boolean> existsByEmail(String email);

    Mono<UserCredentials> findByUsername(String username);

    Mono<Boolean> existsByUsername(String username);

    Mono<UserCredentials> findByProviderId(String providerId);
}
