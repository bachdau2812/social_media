package com.dauducbach.clone.modules.chat.dto.request;

public record MediaMetadataRequest(
        String url,
        String publicId,
        String mimeType,
        Long size,
        String fileName,
        Integer width,
        Integer height,
        Long duration) {
}
