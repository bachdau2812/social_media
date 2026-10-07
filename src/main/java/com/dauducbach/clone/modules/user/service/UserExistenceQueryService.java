package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import com.dauducbach.clone.modules.user.repository.UserDetailsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserExistenceQueryService implements UserExistenceQuery {
    private final UserDetailsRepository userDetailsRepository;

    @Override
    public Mono<Boolean> exists(String userId) {
        return userDetailsRepository.existsById(userId);
    }
}
