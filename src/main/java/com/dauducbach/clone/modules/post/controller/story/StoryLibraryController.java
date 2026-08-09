package com.dauducbach.clone.modules.post.controller.story;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.story.request.StoryHighlightRequest;
import com.dauducbach.clone.modules.post.dto.story.request.StoryReplyRequest;
import com.dauducbach.clone.modules.post.dto.story.response.StoryHighlightResponse;
import com.dauducbach.clone.modules.post.dto.story.response.StoryViewerResponse;
import com.dauducbach.clone.modules.post.dto.story.response.StoryReplyResponse;
import jakarta.validation.Valid;
import com.dauducbach.clone.modules.post.service.story.StoryLibraryService;
import com.dauducbach.clone.modules.post.service.story.StoryReactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
@RequiredArgsConstructor
@RequestMapping("/profile-media")
public class StoryLibraryController {
    private final StoryLibraryService service;
    private final StoryReactionService reactionService;

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

    @PutMapping("/stories/{storyId}/like")
    public Mono<ApiResponse<Boolean>> likeStory(@PathVariable String storyId, Authentication authentication) {
        String actorId = authentication.getName();
        log.info("|StoryLibraryController|likeStory|requested|storyId={}|actorId={}", storyId, actorId);
        return reactionService.like(storyId, actorId)
                .doOnNext(changed -> log.info(
                        "|StoryLibraryController|likeStory|completed|storyId={}|actorId={}|changed={}",
                        storyId, actorId, changed))
                .map(changed -> ApiResponse.<Boolean>builder()
                        .message(changed ? "Story liked" : "Story already liked")
                        .result(changed)
                        .build())
                .doOnError(error -> log.error(
                        "|StoryLibraryController|likeStory|failed|storyId={}|actorId={}|errorType={}",
                        storyId, actorId, error.getClass().getSimpleName()));
    }

    @DeleteMapping("/stories/{storyId}/like")
    public Mono<ApiResponse<Boolean>> unlikeStory(@PathVariable String storyId, Authentication authentication) {
        return reactionService.unlike(storyId, authentication.getName())
                .map(changed -> ApiResponse.<Boolean>builder()
                        .message(changed ? "Story unliked" : "Story was not liked")
                        .result(changed)
                        .build());
    }

    @PostMapping("/stories/{storyId}/replies")
    public Mono<ApiResponse<StoryReplyResponse>> replyStory(
            @PathVariable String storyId,
            @Valid @RequestBody StoryReplyRequest request,
            Authentication authentication
    ) {
        return service.reply(storyId, authentication.getName(), request)
                .map(result -> ApiResponse.<StoryReplyResponse>builder()
                        .message("Story reply sent")
                        .result(result)
                        .build());
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
