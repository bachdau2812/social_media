package com.dauducbach.clone.integration.storychat;

import com.dauducbach.clone.commons.response.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/profile-media")
public class StoryReplyController {
    private final StoryReplyUseCase useCase;

    @PostMapping("/stories/{storyId}/replies")
    public Mono<ApiResponse<StoryReplyResponse>> replyStory(
            @PathVariable String storyId,
            @Valid @RequestBody StoryReplyRequest request,
            Authentication authentication
    ) {
        return useCase.reply(storyId, authentication.getName(), request)
                .map(result -> ApiResponse.<StoryReplyResponse>builder()
                        .message("Story reply sent")
                        .result(result)
                        .build());
    }
}
