package com.dauducbach.clone.commons.response;

import java.util.List;

public record PageResponse<T>(
        List<T> content,
        int pageNumber,
        long totalElements,
        int totalPages
) {
    public static <T> PageResponse<T> of(List<T> content, int pageNumber, long totalElements, int pageSize) {
        int safePageSize = pageSize <= 0 ? 1 : pageSize;
        int totalPages = (int) Math.ceil((double) totalElements / safePageSize);
        return new PageResponse<>(content, Math.max(pageNumber, 0), totalElements, totalPages);
    }
}
