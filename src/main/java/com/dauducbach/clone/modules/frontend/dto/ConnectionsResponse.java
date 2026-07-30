package com.dauducbach.clone.modules.frontend.dto;

import java.util.List;

public record ConnectionsResponse(
        String profileUserId,
        String tab,
        List<ConnectionUserResponse> users,
        int totalCount,
        int currentPage,
        int pageSize,
        boolean hasNextPage,
        boolean hasPreviousPage
) {
}
