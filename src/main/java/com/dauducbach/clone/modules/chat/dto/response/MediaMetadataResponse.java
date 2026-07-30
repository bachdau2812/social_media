package com.dauducbach.clone.modules.chat.dto.response;

public record MediaMetadataResponse(
        String url,
        String publicId,
        String mimeType,
        Long size,
        String fileName,
        Integer width,
        Integer height,
        Long duration) {
}
