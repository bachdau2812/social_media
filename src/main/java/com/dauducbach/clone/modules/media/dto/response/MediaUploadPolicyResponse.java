package com.dauducbach.clone.modules.media.dto.response;

public record MediaUploadPolicyResponse(
        long imageMaxBytes,
        long videoMaxBytes,
        long audioMaxBytes
) {
}
