package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.user.dto.response.UserDiscoveryResponse;
import com.dauducbach.clone.modules.user.service.UserDiscoveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/search/users")
public class UserDiscoveryController {
    private final UserDiscoveryService service;

    @GetMapping("/rich")
    public Mono<ApiResponse<PageResponse<UserDiscoveryResponse>>> search(
            @RequestParam(name = "viewerId", required = false) String viewerId,
            @RequestParam(name = "q") String query,
            @RequestParam(name = "filter", required = false) String filter,
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "20") int size
    ) {
        return service.search(viewerId, query, filter, page, size)
                .map(result -> ApiResponse.<PageResponse<UserDiscoveryResponse>>builder()
                        .message("Rich user search completed successfully")
                        .result(result)
                        .build());
    }

    @GetMapping("/suggested")
    public Mono<ApiResponse<PageResponse<UserDiscoveryResponse>>> suggested(
            @RequestParam(name = "viewerId") String viewerId,
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "20") int size
    ) {
        return service.findSuggested(viewerId, page, size)
                .map(result -> ApiResponse.<PageResponse<UserDiscoveryResponse>>builder()
                        .message("Suggested users fetched successfully")
                        .result(result)
                        .build());
    }

    @PostMapping("/suggested/refresh")
    public Mono<ApiResponse<PageResponse<UserDiscoveryResponse>>> refreshSuggested(
            @RequestParam(name = "viewerId") String viewerId,
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "20") int size
    ) {
        return service.refreshSuggested(viewerId, page, size)
                .map(result -> ApiResponse.<PageResponse<UserDiscoveryResponse>>builder()
                        .message("Suggested users refreshed successfully")
                        .result(result)
                        .build());
    }

    @GetMapping("/{targetUserId}/similar")
    public Mono<ApiResponse<PageResponse<UserDiscoveryResponse>>> similar(
            @PathVariable("targetUserId") String targetUserId,
            @RequestParam(name = "viewerId", required = false) String viewerId,
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "size", defaultValue = "20") int size
    ) {
        return service.findSimilar(viewerId, targetUserId, page, size)
                .map(result -> ApiResponse.<PageResponse<UserDiscoveryResponse>>builder()
                        .message("Similar users fetched successfully")
                        .result(result)
                        .build());
    }
}
