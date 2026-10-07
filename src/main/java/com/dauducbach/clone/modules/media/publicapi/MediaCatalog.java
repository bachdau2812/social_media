package com.dauducbach.clone.modules.media.publicapi;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface MediaCatalog {
    Mono<MediaAssetView> findByPublicId(String publicId);

    Mono<MediaAssetView> findById(String assetId);

    Flux<MediaAssetView> findByIds(Collection<String> assetIds);

    Mono<MediaAssetView> findFirstByOwnerIdAndOwnerType(String ownerId, OwnerType ownerType);

    Flux<MediaAssetView> findByOwnerId(String ownerId, OwnerType ownerType);

    Flux<MediaAssetView> findByOwnerIds(Collection<String> ownerIds, OwnerType ownerType);

    Mono<PageResponse<MediaAssetView>> findProfileMedia(String userId, OwnerType ownerType, int page, int size);

    Mono<MediaAssetView> findCurrentAvatar(String userId);

    Flux<MediaAssetView> findCurrentAvatars(Collection<String> userIds);
}
