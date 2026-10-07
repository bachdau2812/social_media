package com.dauducbach.clone.modules.user.publicapi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface UserDiscoveryQuery {
    Mono<UserDiscoveryProfile> findProfile(String userId);

    Mono<UserDiscoveryResponse> hydrate(String viewerId, String userId);

    Flux<String> findAllUserIds();
}
