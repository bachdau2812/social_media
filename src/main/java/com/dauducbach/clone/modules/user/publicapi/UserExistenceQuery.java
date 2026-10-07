package com.dauducbach.clone.modules.user.publicapi;

import reactor.core.publisher.Mono;

/** Minimal user-presence contract for modules that only need to validate an owner ID. */
public interface UserExistenceQuery {
    Mono<Boolean> exists(String userId);
}
