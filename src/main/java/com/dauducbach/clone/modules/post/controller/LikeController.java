package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.request.LikeRequest;
import com.dauducbach.clone.modules.post.dto.response.LikeToggleResponse;
import com.dauducbach.clone.modules.post.service.LikeService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
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
@RequestMapping("/likes")
public class LikeController {
    private final LikeService likeService;

    @PostMapping("/users/{actorId}")
    public Mono<ApiResponse<LikeToggleResponse>> like(@PathVariable String actorId, @Valid @RequestBody LikeRequest request) {
        return likeService.like(actorId, request)
                .map(response -> ApiResponse.<LikeToggleResponse>builder()
                        .message(response.liked() ? "Liked successfully" : "Unliked successfully")
                        .result(response)
                        .build());
    }

    @GetMapping("/users/{actorId}/status")
    public Mono<ApiResponse<Boolean>> hasLiked(
            @PathVariable String actorId,
            @RequestParam String targetId,
            @RequestParam String targetType
    ) {
        return likeService.hasLiked(actorId, targetId, targetType)
                .map(hasLiked -> ApiResponse.<Boolean>builder()
                        .message("Like status fetched successfully")
                        .result(hasLiked)
                        .build());
    }

    @GetMapping("/count")
    public Mono<ApiResponse<Long>> countLikes(@RequestParam String targetId, @RequestParam String targetType) {
        return likeService.countLikes(targetId, targetType)
                .map(count -> ApiResponse.<Long>builder()
                        .message("Like count fetched successfully")
                        .result(count)
                        .build());
    }

    @GetMapping("/users/{actorId}/targets")
    public Mono<ApiResponse<PageResponse<String>>> getLikedTargets(
            @PathVariable String actorId,
            @RequestParam(defaultValue = "POST") String targetType,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return likeService.getLikedTargets(actorId, targetType, page, size)
                .map(response -> ApiResponse.<PageResponse<String>>builder()
                        .message("Liked targets fetched successfully")
                        .result(response)
                        .build());
    }
}
