package com.dauducbach.clone.modules.user.publicapi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface UserIdentityQuery {
    Mono<Boolean> exists(String userId);

    Mono<String> resolveUsername(String userId);

    Mono<String> resolveDisplayName(String userId);

    Mono<UserIdentity> findIdentity(String userId);

    Mono<UserIdentity> resolveIdentity(String userId);

    Flux<UserIdentity> findIdentities(Collection<String> userIds);
}
