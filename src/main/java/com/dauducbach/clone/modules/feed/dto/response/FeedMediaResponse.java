package com.dauducbach.clone.modules.feed.dto.response;

public record FeedMediaResponse(
        String assetId,
        String publicId,
        String mediaFormat,
        String resourceType,
        String url,
        String secureUrl,
        String displayName
) {
}
