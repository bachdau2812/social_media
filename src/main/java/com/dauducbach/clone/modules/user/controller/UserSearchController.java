package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.user.dto.response.SearchSuggestionResponse;
import com.dauducbach.clone.modules.user.service.SearchSuggestionService;
import com.dauducbach.clone.modules.user.service.UserSearchService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/search")
public class UserSearchController {
    private static final Logger log = LoggerFactory.getLogger(UserSearchController.class);
    private final SearchSuggestionService searchSuggestionService;
    private final UserSearchService userSearchService;

    @GetMapping("/suggestions")
    public Mono<ApiResponse<List<SearchSuggestionResponse>>> getSuggestions(
            @RequestParam(required = false) String userId,
            @RequestParam(defaultValue = "") String q,
            @RequestParam(defaultValue = "10") int limit
    ) {
        return searchSuggestionService.getSuggestions(userId, q, limit)
                .map(response -> ApiResponse.<List<SearchSuggestionResponse>>builder()
                        .message("Search suggestions fetched successfully")
                        .result(response)
                        .build());
    }

    @GetMapping
    public Mono<ApiResponse<PageResponse<String>>> search(
            @RequestParam String userId,
            @RequestParam(name = "q") String query,
            @RequestParam(required = false) String filter,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        log.info("|UserSearchController|search|route=/search|userId={}|queryLength={}|filter={}|page={}|size={}",
                userId, query == null ? 0 : query.length(), filter, page, size);
        return searchSuggestionService.recordSubmittedSearch(userId, query)
                .then(userSearchService.searchUsers(query, filter, page, size))
                .map(response -> ApiResponse.<PageResponse<String>>builder()
                        .message("Search completed successfully")
                        .result(response)
                        .build());
    }

    @DeleteMapping("/history")
    public Mono<ApiResponse<String>> deleteHistory(
            @RequestParam String userId,
            @RequestParam(required = false) String keyword
    ) {
        Mono<Void> deleteAction = keyword == null || keyword.isBlank()
                ? searchSuggestionService.clearHistory(userId)
                : searchSuggestionService.deleteHistoryKeyword(userId, keyword);

        return deleteAction.thenReturn(ApiResponse.<String>builder()
                .message("Search history deleted successfully")
                .result("OK")
                .build());
    }
}
