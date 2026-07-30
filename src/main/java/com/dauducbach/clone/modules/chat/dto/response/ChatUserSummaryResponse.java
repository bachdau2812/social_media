package com.dauducbach.clone.modules.chat.dto.response;

public record ChatUserSummaryResponse(
        String userId,
        String displayName,
        String username,
        String fullName,
        String avatarUrl
) {
}