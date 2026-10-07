package com.dauducbach.clone.modules.media.service;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.infrastructure.cloudinary.CloudinaryMediaService;
import com.dauducbach.clone.modules.media.infrastructure.cloudinary.CloudinaryUrlTransformService;
import com.dauducbach.clone.modules.media.infrastructure.cloudinary.MediaAssetCleanupService;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

@Service
@RequiredArgsConstructor
public class MediaAssetsService implements MediaAssets {
    private final MediaService mediaService;
    private final CloudinaryMediaService cloudinaryMediaService;
    private final CloudinaryUrlTransformService urlTransformService;
    private final MediaAssetCleanupService cleanupService;

    @Override
    public Mono<MediaAssetView> fetchRemoteAsset(String publicId) {
        return cloudinaryMediaService.fetchMediaByPublicId(publicId).map(MediaAssetsService::toView);
    }

    @Override
    public Flux<MediaAssetView> fetchRemoteAssets(List<String> publicIds, String ownerId, OwnerType ownerType) {
        if (publicIds == null || publicIds.isEmpty()) {
            return Flux.empty();
        }
        return cloudinaryMediaService.fetchMediaList(publicIds, ownerId, ownerType)
                .map(MediaAssetsService::toView);
    }

    @Override
    public Mono<MediaAssetView> registerFetchedAsset(
            MediaAssetView fetchedAsset,
            String ownerId,
            OwnerType ownerType) {
        return mediaService.registerFetchedMedia(toEntity(fetchedAsset), ownerId, ownerType)
                .map(MediaAssetsService::toView);
    }

    @Override
    public Mono<MediaAssetView> registerCloudinaryAsset(String publicId, String ownerId, OwnerType ownerType) {
        return mediaService.saveCloudinaryMedia(publicId, ownerId, ownerType)
                .map(MediaAssetsService::toView);
    }

    @Override
    public Mono<Void> deleteAssetsForOwner(String ownerId, OwnerType ownerType) {
        return mediaService.deleteByOwnerIdAndOwnerType(ownerId, ownerType);
    }

    @Override
    public Mono<List<MediaAssetView>> registerCloudinaryAssets(
            List<String> publicIds,
            String ownerId,
            OwnerType ownerType) {
        return mediaService.saveCloudinaryMediaList(publicIds, ownerId, ownerType)
                .map(assets -> assets.stream().map(MediaAssetsService::toView).toList());
    }

    @Override
    public Mono<MediaAssetView> registerFeatureMusic(
            String userId,
            String musicId,
            String displayName,
            String slugName,
            String songUrl,
            String displayImages) {
        return mediaService.saveFeatureMusic(userId, musicId, displayName, slugName, songUrl, displayImages)
                .map(MediaAssetsService::toView);
    }

    @Override
    public Mono<MediaAssetView> registerFetchedMusic(
            String musicId,
            String displayName,
            MediaAudioUploadResult uploadResult) {
        return mediaService.saveFetchedMusicMedia(musicId, displayName, uploadResult)
                .map(MediaAssetsService::toView);
    }

    @Override
    public Mono<Void> deleteAsset(String publicId) {
        return cleanupService.delete(publicId);
    }

    @Override
    public Mono<Void> deleteAssets(Collection<String> publicIds) {
        if (publicIds == null || publicIds.isEmpty()) {
            return Mono.empty();
        }
        return cleanupService.deleteAll(publicIds);
    }

    @Override
    public String transformDeliveryUrl(String mediaUrl, MediaDisplayType displayType) {
        return urlTransformService.transformDeliveryUrl(mediaUrl, displayType);
    }

    @Override
    public String storyVideoStill(String mediaUrl, long previewAtMs) {
        return urlTransformService.storyVideoStill(mediaUrl, previewAtMs);
    }

    @Override
    public void validateMusicSegment(Long musicStart, Long musicEnd) {
        urlTransformService.validateMusicSegment(musicStart, musicEnd);
    }

    @Override
    public String transformMusicUrl(String musicUrl, Long musicStart, Long musicEnd) {
        return urlTransformService.transformMusicUrl(musicUrl, musicStart, musicEnd);
    }

    @Override
    public String transformMusicUrlIfSupported(String musicUrl, Long musicStart, Long musicEnd) {
        return urlTransformService.transformMusicUrlIfSupported(musicUrl, musicStart, musicEnd);
    }

    private static Media toEntity(MediaAssetView asset) {
        if (asset == null) {
            return null;
        }
        return Media.builder()
                .assetId(asset.assetId())
                .publicId(asset.publicId())
                .width(asset.width())
                .height(asset.height())
                .mediaFormat(asset.mediaFormat())
                .resourceType(asset.resourceType())
                .bytes(asset.bytes())
                .url(asset.url())
                .secureUrl(asset.secureUrl())
                .ownerId(asset.ownerId())
                .ownerType(asset.ownerType())
                .version(asset.version())
                .versionId(asset.versionId())
                .displayName(asset.displayName())
                .createdAt(asset.createdAt() == null ? Instant.now() : asset.createdAt())
                .updatedAt(asset.updatedAt() == null ? Instant.now() : asset.updatedAt())
                .build();
    }

    private static MediaAssetView toView(Media media) {
        return new MediaAssetView(
                media.getAssetId(),
                media.getPublicId(),
                media.getWidth(),
                media.getHeight(),
                media.getMediaFormat(),
                media.getResourceType(),
                media.getBytes(),
                media.getUrl(),
                media.getSecureUrl(),
                media.getOwnerId(),
                media.getOwnerType(),
                media.getVersion(),
                media.getVersionId(),
                media.getDisplayName(),
                media.getCreatedAt(),
                media.getUpdatedAt());
    }
}
