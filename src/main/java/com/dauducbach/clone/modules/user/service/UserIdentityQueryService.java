package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import com.dauducbach.clone.modules.media.publicapi.MediaUrlDelivery;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class UserIdentityQueryService implements UserIdentityQuery {
    private final UserDetailsService userDetailsService;
    private final MediaCatalog mediaCatalog;
    private final MediaUrlDelivery mediaUrlDelivery;

    @Override
    public Mono<Boolean> exists(String userId) {
        return userDetailsService.userExists(userId);
    }

    @Override
    public Mono<String> resolveUsername(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just("");
        }
        return userDetailsService.getUserDetailsById(userId)
                .map(details -> firstNonBlank(details.getUsername(), userId))
                .defaultIfEmpty(userId);
    }

    @Override
    public Mono<String> resolveDisplayName(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just("");
        }
        return userDetailsService.getUserDetailsById(userId)
                .map(details -> firstNonBlank(details.getFullName(), details.getUsername(), userId))
                .defaultIfEmpty(userId);
    }

    @Override
    public Mono<UserIdentity> findIdentity(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        Mono<String> avatarUrl = mediaCatalog.findCurrentAvatar(userId)
                .map(this::avatarUrl)
                .defaultIfEmpty("")
                .onErrorReturn("");
        return userDetailsService.getUserDetailsById(userId)
                .flatMap(details -> avatarUrl.map(avatar -> toIdentity(details, avatar)));
    }

    @Override
    public Mono<UserIdentity> resolveIdentity(String userId) {
        String fallbackId = userId == null ? "" : userId.trim();
        return findIdentity(fallbackId)
                .defaultIfEmpty(new UserIdentity(fallbackId, fallbackId, fallbackId, ""));
    }

    @Override
    public Flux<UserIdentity> findIdentities(Collection<String> userIds) {
        List<String> ids = userIds == null ? List.of() : userIds.stream()
                .filter(id -> id != null && !id.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
        if (ids.isEmpty()) {
            return Flux.empty();
        }

        Mono<Map<String, UserDetails>> detailsById = userDetailsService.getUserDetailsByIds(ids)
                .collectMap(UserDetails::getUserId);
        Mono<Map<String, MediaAssetView>> avatarByOwnerId = mediaCatalog.findCurrentAvatars(ids)
                .collectMap(MediaAssetView::ownerId)
                .onErrorReturn(Map.of());

        return Mono.zip(detailsById, avatarByOwnerId)
                .flatMapMany(result -> Flux.fromIterable(ids.stream()
                        .map(id -> {
                            UserDetails details = result.getT1().get(id);
                            return details == null ? null : toIdentity(details, avatarUrl(result.getT2().get(id)));
                        })
                        .filter(identity -> identity != null)
                        .toList()));
    }

    private UserIdentity toIdentity(UserDetails details, String avatar) {
        return new UserIdentity(
                details.getUserId(),
                firstNonBlank(details.getUsername(), details.getUserId()),
                firstNonBlank(details.getFullName(), details.getUsername(), details.getUserId()),
                avatar);
    }

    private String avatarUrl(MediaAssetView media) {
        if (media == null) {
            return "";
        }
        String secure = mediaUrlDelivery.transformDeliveryUrl(media.secureUrl(), MediaDisplayType.AVATAR);
        String url = mediaUrlDelivery.transformDeliveryUrl(media.url(), MediaDisplayType.AVATAR);
        return firstNonBlank(secure, media.secureUrl(), url, media.url());
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }
}
