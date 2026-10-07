package com.dauducbach.clone.modules.auth.recovery;

import reactor.core.publisher.Mono;

import java.time.Duration;

public interface RecoveryChallengeStore {
    Mono<Void> saveRecoveryCode(String email, String code, Duration ttl);

    Mono<String> findRecoveryCode(String email);
}
