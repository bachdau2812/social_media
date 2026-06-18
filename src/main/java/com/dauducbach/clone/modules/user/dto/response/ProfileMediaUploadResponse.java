package com.dauducbach.clone.modules.user.dto.response;

public record ProfileMediaUploadResponse(
        String userId,
        String ownerType,
        String entityId,
        String status,
        String message
) {
}
