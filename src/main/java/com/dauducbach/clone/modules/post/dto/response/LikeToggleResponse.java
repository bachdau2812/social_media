package com.dauducbach.clone.modules.post.dto.response;

public record LikeToggleResponse(
        String targetId,
        String targetType,
        boolean liked,
        String likeId
) {
}
