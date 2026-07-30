package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.user.dto.request.StoryHighlightRequest;
import com.dauducbach.clone.modules.user.dto.response.StoryHighlightResponse;
import com.dauducbach.clone.modules.user.dto.response.StoryViewerResponse;
import com.dauducbach.clone.modules.user.service.StoryLibraryService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/profile-media")
public class StoryLibraryController {
    private final StoryLibraryService service;

    @PostMapping("/stories/{storyId}/views")
    public Mono<ApiResponse<Void>> recordView(
            @PathVariable String storyId,
            @RequestParam String viewerId,
            @RequestParam(required = false) String reaction,
            Authentication authentication
    ) {
        return service.recordView(storyId, authentication.getName(), reaction)
                .thenReturn(ApiResponse.<Void>builder().message("Story view recorded").build());
    }

    @GetMapping("/stories/{storyId}/viewers")
    public Mono<ApiResponse<PageResponse<StoryViewerResponse>>> viewers(
            @PathVariable String storyId,
            @RequestParam String ownerId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            Authentication authentication
    ) {
        return service.viewers(storyId, authentication.getName(), page, size)
                .map(result -> ApiResponse.<PageResponse<StoryViewerResponse>>builder()
                        .message("Story viewers fetched")
                        .result(result)
                        .build());
    }

    @DeleteMapping("/stories/{storyId}")
    public Mono<ApiResponse<Void>> deleteStory(@PathVariable String storyId, Authentication authentication) {
        return service.deleteStory(storyId, authentication.getName())
                .thenReturn(ApiResponse.<Void>builder().message("Story deleted").build());
    }

    @GetMapping("/{ownerId}/highlights")
    public Flux<StoryHighlightResponse> highlights(@PathVariable String ownerId) {
        return service.listHighlights(ownerId);
    }

    @PostMapping("/highlights")
    public Mono<ApiResponse<StoryHighlightResponse>> createHighlight(@RequestBody StoryHighlightRequest request, Authentication authentication) {
        StoryHighlightRequest authenticatedRequest = new StoryHighlightRequest(
                authentication.getName(), request.title(), request.coverStoryId(), request.storyIds());
        return service.createHighlight(authenticatedRequest)
                .map(result -> ApiResponse.<StoryHighlightResponse>builder()
                        .message("Story highlight created")
                        .result(result)
                        .build());
    }

    @PutMapping("/highlights/{highlightId}")
    public Mono<ApiResponse<StoryHighlightResponse>> updateHighlight(
            @PathVariable String highlightId,
            @RequestBody StoryHighlightRequest request,
            Authentication authentication
    ) {
        StoryHighlightRequest authenticatedRequest = new StoryHighlightRequest(
                authentication.getName(), request.title(), request.coverStoryId(), request.storyIds());
        return service.updateHighlight(highlightId, authenticatedRequest)
                .map(result -> ApiResponse.<StoryHighlightResponse>builder()
                        .message("Story highlight updated")
                        .result(result)
                        .build());
    }

    @DeleteMapping("/highlights/{highlightId}")
    public Mono<ApiResponse<Void>> deleteHighlight(
            @PathVariable String highlightId,
            @RequestParam String ownerId,
            Authentication authentication
    ) {
        return service.deleteHighlight(highlightId, authentication.getName())
                .thenReturn(ApiResponse.<Void>builder().message("Story highlight deleted").build());
    }
}
