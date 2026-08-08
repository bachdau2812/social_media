package com.dauducbach.clone.modules.post.controller.story;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.service.story.StoryMediaService;
import com.dauducbach.clone.modules.post.dto.story.request.StoryCreateRequest;
import com.dauducbach.clone.modules.user.dto.response.ProfileMediaUploadResponse;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/profile-media")
public class StoryMediaController {
    private final StoryMediaService storyMediaService;

    @PostMapping("/stories")
    public Mono<ApiResponse<ProfileMediaUploadResponse>> createStory(@Valid @RequestBody StoryCreateRequest request, Authentication authentication) {
        StoryCreateRequest authenticatedRequest = new StoryCreateRequest(
                authentication.getName(), request.mediaUrl(), request.musicId(), request.musicUrl(),
                request.musicStart(), request.musicEnd(), request.publicationId(),
                request.publicationOrder(), request.publicationItemCount());
        return storyMediaService.createStory(authenticatedRequest)
                .map(response -> ApiResponse.<ProfileMediaUploadResponse>builder()
                        .message("Story upload accepted")
                        .result(response)
                        .build());
    }

    @GetMapping("/{userId}/stories")
    public Mono<ApiResponse<PageResponse<UserStories>>> getStories(
            @PathVariable String userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(defaultValue = "STORY") MediaDisplayType mediaType,
            Authentication authentication
    ) {
        return storyMediaService.getStories(userId, authentication.getName(), page, size, mediaType)
                .map(response -> ApiResponse.<PageResponse<UserStories>>builder()
                        .message("Stories fetched")
                        .result(response)
                        .build());
    }
}
