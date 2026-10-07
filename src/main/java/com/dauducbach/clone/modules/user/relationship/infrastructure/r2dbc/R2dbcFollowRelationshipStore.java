package com.dauducbach.clone.modules.user.relationship.infrastructure.r2dbc;

import com.dauducbach.clone.modules.user.entity.UserFollower;
import com.dauducbach.clone.modules.user.relationship.application.FollowRelationshipStore;
import com.dauducbach.clone.modules.user.repository.UserFollowerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class R2dbcFollowRelationshipStore implements FollowRelationshipStore {
    private final UserFollowerRepository userFollowerRepository;
    private final R2dbcEntityTemplate entityTemplate;

    @Override
    public Mono<Boolean> exists(String followerId, String followingId) {
        return userFollowerRepository.existsByFollowerIdAndFollowingId(followerId, followingId);
    }

    @Override
    public Mono<UserFollower> insert(UserFollower relationship) {
        return entityTemplate.insert(UserFollower.class).using(relationship);
    }

    @Override
    public Mono<Void> delete(String followerId, String followingId) {
        return userFollowerRepository.deleteByFollowerIdAndFollowingId(followerId, followingId);
    }
}
