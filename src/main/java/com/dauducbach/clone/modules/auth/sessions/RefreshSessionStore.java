package com.dauducbach.clone.modules.auth.sessions;

import reactor.core.publisher.Mono;

public interface RefreshSessionStore {
    Mono<RefreshSession> findCurrentValid(String tokenHash, String deviceInfo);

    Mono<Integer> revokeCurrent(String tokenHash, String deviceInfo);

    Mono<Integer> revokeAllForUser(String userId);

    Mono<Void> save(RefreshSession session);
}
