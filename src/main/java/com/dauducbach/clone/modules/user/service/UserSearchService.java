package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.infrastructure.service.SemanticVectorSearchService;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserSearchService {
    private static final Logger log = LoggerFactory.getLogger(UserSearchService.class);
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 100;
    private static final int SEMANTIC_FILL_THRESHOLD = 10;

    UserDetailsRepository userDetailsRepository;
    SemanticVectorSearchService semanticVectorSearchService;

    public Mono<PageResponse<String>> searchUsers(String query, String filter, int page, int limit) {
        String normalizedQuery = normalizeQuery(query);
        SearchFilter searchFilter = parseFilter(filter);
        int pageNumber = Math.max(page, 0);
        int pageSize = normalizeLimit(limit);
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        log.info("|UserSearchService|searchUsers|queryLength={}|filter={}|page={}|limit={}",
                normalizedQuery.length(), filter, pageNumber, pageSize);

        return userDetailsRepository.countSearchUserIds(
                        normalizedQuery,
                        searchFilter.includeHobby(),
                        searchFilter.includeLivingIn(),
                        searchFilter.includeHometown(),
                        searchFilter.includeSex()
                )
                .flatMap(total -> userDetailsRepository.searchUserIds(
                                normalizedQuery,
                                searchFilter.includeHobby(),
                                searchFilter.includeLivingIn(),
                                searchFilter.includeHometown(),
                                searchFilter.includeSex(),
                                pageable
                        )
                        .collectList()
                        .doOnSuccess(dbUserIds -> log.info("|UserSearchService|searchUsers|dbResult|page={}|dbCount={}|dbTotal={}",
                                pageNumber, dbUserIds.size(), total))
                        .flatMap(dbUserIds -> fillUsersBySemanticSearch(normalizedQuery, dbUserIds, pageSize)
                                .map(content -> buildPage(content, dbUserIds.size(), pageNumber, pageSize, total))))
                .doOnSuccess(response -> log.info("|UserSearchService|searchUsers|completed|page={}|resultCount={}|totalElements={}",
                        pageNumber, response.content().size(), response.totalElements()))
                .doOnError(error -> log.error("|UserSearchService|searchUsers|failed|page={}|limit={}|error={}",
                        pageNumber, pageSize, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.USER_SEARCH_FAILED,
                                String.format("Search users failed: query=%s filter=%s page=%d limit=%d",
                                        normalizedQuery, filter, pageNumber, pageSize),
                                error
                        ));
    }

    private Mono<List<String>> fillUsersBySemanticSearch(String query, List<String> dbUserIds, int pageSize) {
        if (dbUserIds.size() >= SEMANTIC_FILL_THRESHOLD || dbUserIds.size() >= pageSize) {
            return Mono.just(dbUserIds);
        }

        int required = pageSize - dbUserIds.size();
        Set<String> excludedIds = new LinkedHashSet<>(dbUserIds);

        log.info("|UserSearchService|fillUsersBySemanticSearch|start|dbCount={}|required={}",
                dbUserIds.size(), required);

        return semanticVectorSearchService.searchUserIds(query, required, excludedIds)
                .doOnSuccess(semanticUserIds -> log.info("|UserSearchService|fillUsersBySemanticSearch|completed|semanticCount={}",
                        semanticUserIds.size()))
                .doOnError(error -> log.error("|UserSearchService|fillUsersBySemanticSearch|failed|required={}|error={}",
                        required, error.getMessage()))
                .map(semanticUserIds -> mergeIds(dbUserIds, semanticUserIds, pageSize));
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

    private SearchFilter parseFilter(String filter) {
        if (filter == null || filter.isBlank()) {
            return SearchFilter.empty();
        }

        List<String> tokens = Arrays.stream(filter.split("[+\\s,]+"))
                .map(token -> token.trim().toLowerCase(Locale.ROOT))
                .filter(token -> !token.isBlank())
                .toList();

        boolean includeHobby = false;
        boolean includeLivingIn = false;
        boolean includeHometown = false;
        boolean includeSex = false;

        for (String token : tokens) {
            switch (token) {
                case "hobby", "hobbies", "hobby_list", "hobbie", "hobbie_list" -> includeHobby = true;
                case "living_in", "live_in", "livingin" -> includeLivingIn = true;
                case "hometown", "home_town" -> includeHometown = true;
                case "city" -> {
                    includeLivingIn = true;
                    includeHometown = true;
                }
                case "sex", "gender" -> includeSex = true;
                default -> {
                    // Unknown filters are ignored because only whitelisted columns may affect SQL.
                }
            }
        }

        return new SearchFilter(flag(includeHobby), flag(includeLivingIn), flag(includeHometown), flag(includeSex));
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

    private int flag(boolean value) {
        return value ? 1 : 0;
    }

    private record SearchFilter(int includeHobby, int includeLivingIn, int includeHometown, int includeSex) {
        private static SearchFilter empty() {
            return new SearchFilter(0, 0, 0, 0);
        }
    }
}
