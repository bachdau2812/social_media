package com.dauducbach.clone.modules.user.profile.application;

import com.dauducbach.clone.modules.user.entity.UserDetails;
import reactor.core.publisher.Mono;

public interface ProfileCache {
    Mono<UserDetails> find(String userId);

    Mono<Void> put(UserDetails userDetails);

    Mono<Void> evict(String userId);
}
