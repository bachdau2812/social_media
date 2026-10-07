package com.dauducbach.clone.modules.media.publicapi;

import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public interface MediaAssetRegistry {
    Mono<MediaAssetView> fetchRemoteAsset(String publicId);
    Flux<MediaAssetView> fetchRemoteAssets(List<String> publicIds, String ownerId, OwnerType ownerType);
    Mono<MediaAssetView> registerFetchedAsset(MediaAssetView fetchedAsset, String ownerId, OwnerType ownerType);
    Mono<MediaAssetView> registerCloudinaryAsset(String publicId, String ownerId, OwnerType ownerType);
    Mono<List<MediaAssetView>> registerCloudinaryAssets(List<String> publicIds, String ownerId, OwnerType ownerType);
    Mono<MediaAssetView> registerFeatureMusic(String userId, String musicId, String displayName,
            String slugName, String songUrl, String displayImages);
    Mono<MediaAssetView> registerFetchedMusic(String musicId, String displayName, MediaAudioUploadResult uploadResult);
}
