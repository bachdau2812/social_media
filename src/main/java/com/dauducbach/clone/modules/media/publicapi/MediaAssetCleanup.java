package com.dauducbach.clone.modules.media.publicapi;

import com.dauducbach.clone.modules.media.constant.OwnerType;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface MediaAssetCleanup {
    Mono<Void> deleteAssetsForOwner(String ownerId, OwnerType ownerType);

    Mono<Void> deleteAsset(String publicId);

    Mono<Void> deleteAssets(Collection<String> publicIds);
}
