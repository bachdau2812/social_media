package com.dauducbach.clone.modules.user.dto.response;

public record ChatUserSuggestionResponse(
        String id,
        String username,
        String fullName,
        String avatar
) {
}
