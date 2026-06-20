package com.dauducbach.clone.modules.user.dto.response;

public record SearchSuggestionResponse(
        String text,
        String source,
        boolean isHistory
) {
}
