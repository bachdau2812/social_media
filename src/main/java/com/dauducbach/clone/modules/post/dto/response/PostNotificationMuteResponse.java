package com.dauducbach.clone.modules.post.dto.response;

public record PostNotificationMuteResponse(
        String postId,
        String userId,
        long mutedDays
) {
}
