package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.user.dto.request.UserSocialMediaRequest;
import com.dauducbach.clone.modules.user.entity.UserSocialMedia;
import com.dauducbach.clone.modules.user.service.UserSocialMediaService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/user-social-media")
public class UserSocialMediaController {
    private final UserSocialMediaService userSocialMediaService;

    /// Tạo mới UserSocialMedia
    @PostMapping
    public Mono<ApiResponse<UserSocialMedia>> createUserSocialMedia(@Valid @RequestBody UserSocialMediaRequest request) {
        return userSocialMediaService.createUserSocialMedia(request)
                .map(createdSocialMedia -> ApiResponse.<UserSocialMedia>builder()
                        .message("UserSocialMedia created successfully")
                        .result(createdSocialMedia)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserSocialMedia>builder()
                        .message("Error creating UserSocialMedia: " + error.getMessage())
                        .build()));
    }

    /// Lấy UserSocialMedia theo ID
    @GetMapping("/{id}")
    public Mono<ApiResponse<UserSocialMedia>> getUserSocialMediaById(@PathVariable String id) {
        return userSocialMediaService.getUserSocialMediaById(id)
                .map(socialMedia -> ApiResponse.<UserSocialMedia>builder()
                        .message("UserSocialMedia retrieved successfully")
                        .result(socialMedia)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserSocialMedia>builder()
                        .message("Error retrieving UserSocialMedia: " + error.getMessage())
                        .build()));
    }

    /// Lấy danh sách UserSocialMedia của user
    @GetMapping("/user/{userId}")
    public Flux<UserSocialMedia> getUserSocialMediaByUserId(@PathVariable String userId) {
        return userSocialMediaService.getUserSocialMediaByUserId(userId);
    }

    /// Xóa UserSocialMedia
    @DeleteMapping("/{id}")
    public Mono<ApiResponse<String>> deleteUserSocialMedia(@PathVariable String id) {
        return userSocialMediaService.deleteUserSocialMedia(id)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("UserSocialMedia deleted successfully")
                        .result("UserSocialMedia with ID: " + id + " has been deleted")
                        .build()))
                .onErrorResume(error -> Mono.just(ApiResponse.<String>builder()
                        .message("Error deleting UserSocialMedia: " + error.getMessage())
                        .build()));
    }
}