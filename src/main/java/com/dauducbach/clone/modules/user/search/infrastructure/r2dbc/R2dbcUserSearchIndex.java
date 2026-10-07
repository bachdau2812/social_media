package com.dauducbach.clone.modules.user.search.infrastructure.r2dbc;

import com.dauducbach.clone.modules.user.repository.UserDetailsRepository;
import com.dauducbach.clone.modules.user.search.application.UserSearchCriteria;
import com.dauducbach.clone.modules.user.search.application.UserSearchIndex;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class R2dbcUserSearchIndex implements UserSearchIndex {
    private final UserDetailsRepository userDetailsRepository;

    @Override
    public Mono<Long> count(UserSearchCriteria criteria) {
        return userDetailsRepository.countSearchUserIds(
                criteria.queryPattern(),
                flag(criteria.includeHobby()),
                flag(criteria.includeLivingIn()),
                flag(criteria.includeHometown()),
                flag(criteria.includeSex()));
    }

    @Override
    public Flux<String> findIds(UserSearchCriteria criteria) {
        return userDetailsRepository.searchUserIds(
                criteria.queryPattern(),
                flag(criteria.includeHobby()),
                flag(criteria.includeLivingIn()),
                flag(criteria.includeHometown()),
                flag(criteria.includeSex()),
                PageRequest.of(criteria.page(), criteria.size()));
    }

    private int flag(boolean enabled) {
        return enabled ? 1 : 0;
    }
}
