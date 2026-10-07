package com.dauducbach.clone.modules.media.publicapi;

import com.dauducbach.clone.modules.media.constant.OwnerType;

import java.time.Instant;

/** Read model shared with feature modules; it deliberately has no persistence annotations. */
public record MediaAssetView(
        String assetId,
        String publicId,
        int width,
        int height,
        String mediaFormat,
        String resourceType,
        int bytes,
        String url,
        String secureUrl,
        String ownerId,
        OwnerType ownerType,
        String version,
        String versionId,
        String displayName,
        Instant createdAt,
        Instant updatedAt) {

    public MediaAssetView withDeliveryUrls(String url, String secureUrl) {
        return new MediaAssetView(
                assetId, publicId, width, height, mediaFormat, resourceType, bytes,
                url, secureUrl, ownerId, ownerType, version, versionId, displayName,
                createdAt, updatedAt);
    }
}
