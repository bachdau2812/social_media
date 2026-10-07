package com.dauducbach.clone.modules.auth.registration;

import reactor.core.publisher.Mono;

import java.time.Duration;

public interface RegistrationChallengeStore {
    Mono<Void> saveDraft(String email, RegistrationDraft draft, Duration ttl);

    Mono<RegistrationDraft> findDraft(String email);

    Mono<Void> saveRegistrationCode(String email, String code, Duration ttl);

    Mono<String> findRegistrationCode(String email);
}
