package com.dauducbach.clone.modules.media.dto.response;

public record MediaAudioUploadResult(
        String assetId,
        String publicId,
        int width,
        int height,
        String mediaFormat,
        String resourceType,
        int bytes,
        String url,
        String secureUrl,
        String version,
        String versionId
) {
}
