package com.dauducbach.clone.modules.user.search.application;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface UserSearchIndex {
    Mono<Long> count(UserSearchCriteria criteria);

    Flux<String> findIds(UserSearchCriteria criteria);
}
