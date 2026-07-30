package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.user.dto.response.UserDiscoveryResponse;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.repositoty.UserFollowerRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class UserDiscoveryHydrator {
    private final UserDetailsService userDetailsService;
    private final MediaForProfile mediaForProfile;
    private final UserFollowerRepository followerRepository;

    public Mono<UserDiscoveryResponse> hydrate(String viewerId, String userId) {
        Mono<Optional<Media>> avatar = mediaForProfile.getCurrentAvatar(userId, MediaDisplayType.AVATAR)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .onErrorReturn(Optional.empty());
        Mono<Boolean> viewerFollows = follows(viewerId, userId);
        Mono<Boolean> followsViewer = follows(userId, viewerId);

        return Mono.zip(userDetailsService.getUserDetailsById(userId), avatar, viewerFollows, followsViewer)
                .map(tuple -> toResponse(
                        viewerId,
                        tuple.getT1(),
                        tuple.getT2(),
                        Boolean.TRUE.equals(tuple.getT3()),
                        Boolean.TRUE.equals(tuple.getT4())
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
                                             UserDetails details,
                                             Optional<Media> avatar,
                                             boolean viewerFollows,
                                             boolean followsViewer) {
        String userId = details.getUserId();
        String username = firstNonBlank(details.getUsername(), userId);
        String fullName = firstNonBlank(details.getFullName(), username, userId);
        String avatarUrl = avatar.map(value -> firstNonBlank(value.getSecureUrl(), value.getUrl())).orElse("");
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
