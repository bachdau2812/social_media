package com.dauducbach.clone.modules.auth.sessions;

import reactor.core.publisher.Mono;

import java.time.Duration;

public interface AccessTokenRevocationStore {
    Mono<Boolean> revoke(String accessToken, Duration ttl);

    Mono<Boolean> isRevoked(String accessToken);

    Mono<Boolean> isUserRevoked(String userId);
}
