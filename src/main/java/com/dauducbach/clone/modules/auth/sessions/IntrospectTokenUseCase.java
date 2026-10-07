package com.dauducbach.clone.modules.auth.sessions;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class IntrospectTokenUseCase {
    private static final Logger log = LoggerFactory.getLogger(IntrospectTokenUseCase.class);

    private final AuthTokenProvider tokenProvider;

    public Mono<Boolean> isValid(String token) {
        return tokenProvider.verifyAccessToken(token)
                .doOnSuccess(valid -> log.info("|IntrospectTokenUseCase|completed|valid={}", valid))
                .onErrorResume(error -> {
                    log.warn("|IntrospectTokenUseCase|verification failed|errorType={}",
                            error.getClass().getSimpleName());
                    return Mono.just(false);
                });
    }
}
