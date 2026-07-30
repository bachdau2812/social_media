package com.dauducbach.clone.modules.chat.dto.response;

import java.util.List;

public record MemberRequestPageResponse(
        List<MemberRequestResponse> items,
        int page,
        int size,
        long total,
        boolean hasNext
) {
}