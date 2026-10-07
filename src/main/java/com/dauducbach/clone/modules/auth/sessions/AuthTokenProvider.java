package com.dauducbach.clone.modules.auth.sessions;

import reactor.core.publisher.Mono;

public interface AuthTokenProvider {
    Mono<String> issueAccessToken(String userId, String role);

    Mono<Boolean> verifyAccessToken(String token);
}
