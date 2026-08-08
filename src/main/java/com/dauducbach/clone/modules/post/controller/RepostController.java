package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.commons.security.ActorIdentity;
import com.dauducbach.clone.modules.post.dto.response.RepostToggleResponse;
import com.dauducbach.clone.modules.post.service.post.RepostService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/posts")
public class RepostController {
    private final RepostService repostService;

    @PostMapping("/{postId}/repost")
    public Mono<ApiResponse<RepostToggleResponse>> repost(@PathVariable String postId, @RequestParam String actorId, Authentication authentication) {
        return repostService.repost(requireActor(authentication, actorId), postId)
                .map(response -> ApiResponse.<RepostToggleResponse>builder()
                        .message("Post reposted successfully")
                        .result(response)
                        .build());
    }

    @DeleteMapping("/{postId}/repost")
    public Mono<ApiResponse<RepostToggleResponse>> unrepost(@PathVariable String postId, @RequestParam String actorId, Authentication authentication) {
        return repostService.unrepost(requireActor(authentication, actorId), postId)
                .map(response -> ApiResponse.<RepostToggleResponse>builder()
                        .message("Post repost removed successfully")
                        .result(response)
                        .build());
    }

    @GetMapping("/{postId}/repost/status")
    public Mono<ApiResponse<Boolean>> hasReposted(@PathVariable String postId, @RequestParam String actorId, Authentication authentication) {
        return repostService.hasReposted(requireActor(authentication, actorId), postId)
                .map(response -> ApiResponse.<Boolean>builder()
                        .message("Repost status fetched successfully")
                        .result(response)
                        .build());
    }

    @GetMapping("/{postId}/repost/count")
    public Mono<ApiResponse<Long>> countReposts(@PathVariable String postId) {
        return repostService.countReposts(postId)
                .map(response -> ApiResponse.<Long>builder()
                        .message("Repost count fetched successfully")
                        .result(response)
                        .build());
    }

    @GetMapping("/{postId}/reposts/actors")
    public Mono<ApiResponse<PageResponse<String>>> getReposterActorIds(
            @PathVariable String postId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return repostService.getReposterActorIds(postId, page, size)
                .map(response -> ApiResponse.<PageResponse<String>>builder()
                        .message("Reposter actors fetched successfully")
                        .result(response)
                        .build());
    }
    @GetMapping("/users/{actorId}/reposts")
    public Mono<ApiResponse<PageResponse<String>>> getRepostedPostIds(
            @PathVariable String actorId,
            Authentication authentication,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return repostService.getRepostedPostIds(requireActor(authentication, actorId), page, size)
                .map(response -> ApiResponse.<PageResponse<String>>builder()
                        .message("Reposted posts fetched successfully")
                        .result(response)
                        .build());
    }
    private String requireActor(Authentication authentication, String actorId) {
        return ActorIdentity.require(authentication.getName(), actorId);
    }
}

