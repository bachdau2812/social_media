package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserIdentityQueryService {
    private final UserDetailsService userDetailsService;
    private final MediaForProfile mediaForProfile;

    public Mono<String> resolveUsername(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just("");
        }
        return userDetailsService.getUserDetailsById(userId)
                .map(details -> firstNonBlank(details.getUsername(), userId))
                .defaultIfEmpty(userId);
    }

    public Mono<String> resolveDisplayName(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just("");
        }
        return userDetailsService.getUserDetailsById(userId)
                .map(details -> firstNonBlank(details.getFullName(), details.getUsername(), userId))
                .defaultIfEmpty(userId);
    }

    public Mono<IdentitySnapshot> findIdentity(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        Mono<String> avatarUrl = mediaForProfile.getCurrentAvatar(userId, MediaDisplayType.AVATAR)
                .map(media -> firstNonBlank(media.getSecureUrl(), media.getUrl()))
                .defaultIfEmpty("")
                .onErrorReturn("");
        return userDetailsService.getUserDetailsById(userId)
                .flatMap(details -> avatarUrl.map(avatar -> new IdentitySnapshot(
                        details.getUserId(),
                        firstNonBlank(details.getUsername(), userId),
                        firstNonBlank(details.getFullName(), details.getUsername(), userId),
                        avatar
                )));
    }

    public Mono<IdentitySnapshot> resolveIdentity(String userId) {
        String fallbackId = userId == null ? "" : userId.trim();
        return findIdentity(fallbackId)
                .defaultIfEmpty(new IdentitySnapshot(fallbackId, fallbackId, fallbackId, ""));
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }

    public record IdentitySnapshot(
            String userId,
            String username,
            String fullName,
            String avatarUrl
    ) {
    }
}
