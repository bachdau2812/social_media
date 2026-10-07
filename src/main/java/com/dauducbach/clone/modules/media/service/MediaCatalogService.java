package com.dauducbach.clone.modules.media.service;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

@Service
@RequiredArgsConstructor
public class MediaCatalogService implements MediaCatalog {
    private final MediaService mediaService;

    @Override
    public Mono<MediaAssetView> findByPublicId(String publicId) {
        return mediaService.getByPublicId(publicId).map(MediaCatalogService::toView);
    }

    @Override
    public Mono<MediaAssetView> findById(String assetId) {
        return mediaService.getById(assetId).map(MediaCatalogService::toView);
    }

    @Override
    public Flux<MediaAssetView> findByIds(Collection<String> assetIds) {
        return mediaService.getByIds(assetIds).map(MediaCatalogService::toView);
    }

    @Override
    public Mono<MediaAssetView> findFirstByOwnerIdAndOwnerType(String ownerId, OwnerType ownerType) {
        return mediaService.getFirstByOwnerIdAndOwnerType(ownerId, ownerType).map(MediaCatalogService::toView);
    }

    @Override
    public Flux<MediaAssetView> findByOwnerId(String ownerId, OwnerType ownerType) {
        return mediaService.getByOwnerId(ownerId, ownerType).map(MediaCatalogService::toView);
    }

    @Override
    public Flux<MediaAssetView> findByOwnerIds(Collection<String> ownerIds, OwnerType ownerType) {
        return mediaService.getByOwnerIds(ownerIds, ownerType).map(MediaCatalogService::toView);
    }

    @Override
    public Mono<PageResponse<MediaAssetView>> findProfileMedia(
            String userId,
            OwnerType ownerType,
            int page,
            int size) {
        return mediaService.getProfileMedia(userId, ownerType, page, size)
                .map(result -> new PageResponse<>(
                        result.content().stream().map(MediaCatalogService::toView).toList(),
                        result.pageNumber(),
                        result.totalElements(),
                        result.totalPages()));
    }

    @Override
    public Mono<MediaAssetView> findCurrentAvatar(String userId) {
        return mediaService.getCurrentAvatar(userId).map(MediaCatalogService::toView);
    }

    @Override
    public Flux<MediaAssetView> findCurrentAvatars(Collection<String> userIds) {
        return mediaService.getCurrentAvatars(userIds).map(MediaCatalogService::toView);
    }

    private static MediaAssetView toView(Media media) {
        return new MediaAssetView(
                media.getAssetId(), media.getPublicId(), media.getWidth(), media.getHeight(),
                media.getMediaFormat(), media.getResourceType(), media.getBytes(), media.getUrl(),
                media.getSecureUrl(), media.getOwnerId(), media.getOwnerType(), media.getVersion(),
                media.getVersionId(), media.getDisplayName(), media.getCreatedAt(), media.getUpdatedAt());
    }
}
