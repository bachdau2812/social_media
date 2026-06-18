package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.infrastructure.service.SemanticVectorSearchService;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class PostSearchService {
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 100;
    private static final int SEMANTIC_FILL_THRESHOLD = 10;

    PostDetailsRepository postDetailsRepository;
    SemanticVectorSearchService semanticVectorSearchService;

    public Mono<PageResponse<String>> searchPosts(String query, int page, int limit) {
        String normalizedQuery = normalizeQuery(query);
        int pageNumber = Math.max(page, 0);
        int pageSize = normalizeLimit(limit);
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        return postDetailsRepository.countSearchApprovedPostIds(normalizedQuery)
                .flatMap(total -> postDetailsRepository.searchApprovedPostIds(normalizedQuery, pageable)
                        .collectList()
                        .flatMap(dbPostIds -> fillPostsBySemanticSearch(normalizedQuery, dbPostIds, pageSize)
                                .map(content -> buildPage(content, dbPostIds.size(), pageNumber, pageSize, total))))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.POST_SEARCH_FAILED,
                                String.format("Search posts failed: query=%s page=%d limit=%d",
                                        normalizedQuery, pageNumber, pageSize),
                                error
                        ));
    }

    private Mono<List<String>> fillPostsBySemanticSearch(String query, List<String> dbPostIds, int pageSize) {
        if (dbPostIds.size() >= SEMANTIC_FILL_THRESHOLD || dbPostIds.size() >= pageSize) {
            return Mono.just(dbPostIds);
        }

        int required = pageSize - dbPostIds.size();
        Set<String> excludedIds = new LinkedHashSet<>(dbPostIds);

        return semanticVectorSearchService.searchPostIds(query, required, excludedIds)
                .map(semanticPostIds -> mergeIds(dbPostIds, semanticPostIds, pageSize));
    }

    private PageResponse<String> buildPage(List<String> content, int dbPageCount, int pageNumber, int pageSize, long dbTotal) {
        long semanticFillCount = Math.max(content.size() - dbPageCount, 0);
        long totalElements = Math.max(dbTotal, (long) pageNumber * pageSize + content.size());
        if (semanticFillCount > 0) {
            totalElements = Math.max(totalElements, dbTotal + semanticFillCount);
        }
        return PageResponse.of(content, pageNumber, totalElements, pageSize);
    }

    private List<String> mergeIds(List<String> dbIds, List<String> semanticIds, int pageSize) {
        LinkedHashSet<String> merged = new LinkedHashSet<>(dbIds);
        merged.addAll(semanticIds);
        return merged.stream().limit(pageSize).toList();
    }

    private String normalizeQuery(String query) {
        if (query == null || query.isBlank()) {
            throw new AppException(ErrorCode.SEARCH_REQUEST_INVALID, "query is required");
        }
        return query.trim();
    }

    private int normalizeLimit(int limit) {
        if (limit <= 0) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(limit, MAX_PAGE_SIZE);
    }
}
