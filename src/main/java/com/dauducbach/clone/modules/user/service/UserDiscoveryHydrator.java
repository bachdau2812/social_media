package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryResponse;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import com.dauducbach.clone.modules.user.repository.UserFollowerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserDiscoveryHydrator {
    private final UserIdentityQuery userIdentityQuery;
    private final UserFollowerRepository followerRepository;

    public Mono<UserDiscoveryResponse> hydrate(String viewerId, String userId) {
        Mono<Boolean> viewerFollows = follows(viewerId, userId);
        Mono<Boolean> followsViewer = follows(userId, viewerId);

        return Mono.zip(userIdentityQuery.findIdentity(userId), viewerFollows, followsViewer)
                .map(tuple -> toResponse(
                        viewerId,
                        tuple.getT1(),
                        Boolean.TRUE.equals(tuple.getT2()),
                        Boolean.TRUE.equals(tuple.getT3())
                ));
    }

    private Mono<Boolean> follows(String followerId, String followingId) {
        if (!hasText(followerId) || !hasText(followingId) || followerId.equals(followingId)) {
            return Mono.just(false);
        }
        return followerRepository.existsByFollowerIdAndFollowingId(followerId, followingId)
                .defaultIfEmpty(false)
                .onErrorReturn(false);
    }

    private UserDiscoveryResponse toResponse(String viewerId,
                                             UserIdentity identity,
                                             boolean viewerFollows,
                                             boolean followsViewer) {
        String userId = identity.userId();
        String username = firstNonBlank(identity.username(), userId);
        String fullName = firstNonBlank(identity.fullName(), username, userId);
        String avatarUrl = firstNonBlank(identity.avatarUrl());
        boolean friend = viewerFollows && followsViewer;
        String relationship = userId.equals(viewerId)
                ? "SELF"
                : friend
                ? "FRIEND"
                : viewerFollows
                ? "FOLLOWING"
                : followsViewer ? "FOLLOWS_YOU" : "NONE";
        return new UserDiscoveryResponse(
                userId, username, fullName, avatarUrl,
                viewerFollows, followsViewer, friend, relationship
        );
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (hasText(value)) return value.trim();
        }
        return "";
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
