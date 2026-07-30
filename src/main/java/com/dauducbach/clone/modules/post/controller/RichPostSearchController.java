package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.response.RichPostSearchResponse;
import com.dauducbach.clone.modules.post.service.RichPostSearchService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/posts/search")
public class RichPostSearchController {
    private final RichPostSearchService service;

    @GetMapping("/rich")
    public Mono<ApiResponse<PageResponse<RichPostSearchResponse>>> search(
            @RequestParam("query") String query,
            @RequestParam(name = "page", defaultValue = "0") int page,
            @RequestParam(name = "limit", defaultValue = "20") int limit
    ) {
        return service.search(query, page, limit)
                .map(result -> ApiResponse.<PageResponse<RichPostSearchResponse>>builder()
                        .message("Rich post search completed successfully")
                        .result(result)
                        .build());
    }
}
