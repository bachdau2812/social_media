package com.dauducbach.clone.modules.feed.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedLongTermVectorRefreshResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.feed.service.FeedLongTermVectorService;
import com.dauducbach.clone.modules.feed.service.FeedService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/feed")
public class FeedController {
    private final FeedService feedService;
    private final FeedLongTermVectorService feedLongTermVectorService;

    @GetMapping
    public Mono<ApiResponse<FeedResponse>> getFeed(@RequestParam String userId,
                                                   @RequestParam(defaultValue = "20") int limit) {
        return feedService.getFeed(userId, limit)
                .map(response -> ApiResponse.<FeedResponse>builder()
                        .message("Feed retrieved successfully")
                        .result(response)
                        .build());
    }

    @PostMapping("/long-term-vectors/refresh")
    public Mono<ApiResponse<FeedLongTermVectorRefreshResponse>> refreshLongTermVectors(
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String userId
    ) {
        return feedLongTermVectorService.refreshLongTermVectors(from, to, userId)
                .map(response -> ApiResponse.<FeedLongTermVectorRefreshResponse>builder()
                        .message("Feed long term vectors refreshed successfully")
                        .result(response)
                        .build());
    }
}
