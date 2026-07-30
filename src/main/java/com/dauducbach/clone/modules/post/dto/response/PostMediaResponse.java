package com.dauducbach.clone.modules.post.dto.response;

public record PostMediaResponse(
        String assetId,
        String publicId,
        String mediaFormat,
        String resourceType,
        String url,
        String secureUrl,
        String displayName,
        int width,
        int height
) {
}
