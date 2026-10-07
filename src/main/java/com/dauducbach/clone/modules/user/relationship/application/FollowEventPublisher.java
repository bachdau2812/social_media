package com.dauducbach.clone.modules.user.relationship.application;

import reactor.core.publisher.Mono;

public interface FollowEventPublisher {
    Mono<Void> followed(String followerId, String followingId);

    Mono<Void> unfollowed(String followerId, String followingId);
}
