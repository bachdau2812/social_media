package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryProfile;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryQuery;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryResponse;
import com.dauducbach.clone.modules.user.repository.UserDetailsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserDiscoveryQueryService implements UserDiscoveryQuery {
    private final UserDetailsRepository users;
    private final UserDiscoveryHydrator hydrator;

    @Override
    public Mono<UserDiscoveryProfile> findProfile(String userId) {
        return users.findById(userId).map(details -> new UserDiscoveryProfile(
                details.getUserId(), details.getLivingIn(), details.getHometown(), details.getHobbyList()));
    }

    @Override
    public Mono<UserDiscoveryResponse> hydrate(String viewerId, String userId) {
        return hydrator.hydrate(viewerId, userId);
    }

    @Override
    public Flux<String> findAllUserIds() {
        return users.findAllUserIds();
    }
}
