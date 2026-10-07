package com.dauducbach.clone.modules.user.relationship.application;

import com.dauducbach.clone.modules.user.entity.UserFollower;
import reactor.core.publisher.Mono;

public interface FollowRelationshipStore {
    Mono<Boolean> exists(String followerId, String followingId);

    Mono<UserFollower> insert(UserFollower relationship);

    Mono<Void> delete(String followerId, String followingId);
}
