package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.user.dto.request.AvatarUploadRequest;
import com.dauducbach.clone.modules.user.dto.request.MusicSelectRequest;
import com.dauducbach.clone.modules.user.dto.request.StoryCreateRequest;
import com.dauducbach.clone.modules.user.dto.response.ProfileMediaUploadResponse;
import com.dauducbach.clone.modules.user.entity.UserMusics;
import com.dauducbach.clone.modules.user.entity.UserStories;
import com.dauducbach.clone.modules.user.service.MediaForProfile;
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
public class MediaForProfileController {
    private final MediaForProfile mediaForProfile;

    @PostMapping("/avatar")
    public Mono<ApiResponse<ProfileMediaUploadResponse>> uploadAvatar(@Valid @RequestBody AvatarUploadRequest request) {
        return mediaForProfile.uploadAvatar(request)
                .map(response -> ApiResponse.<ProfileMediaUploadResponse>builder()
                        .message("Avatar upload accepted")
                        .result(response)
                        .build());
    }

    @PostMapping("/stories")
    public Mono<ApiResponse<ProfileMediaUploadResponse>> createStory(@Valid @RequestBody StoryCreateRequest request, Authentication authentication) {
        StoryCreateRequest authenticatedRequest = new StoryCreateRequest(
                authentication.getName(), request.mediaUrl(), request.musicId(), request.musicUrl(),
                request.musicStart(), request.musicEnd(), request.publicationId(),
                request.publicationOrder(), request.publicationItemCount());
        return mediaForProfile.createStory(authenticatedRequest)
                .map(response -> ApiResponse.<ProfileMediaUploadResponse>builder()
                        .message("Story upload accepted")
                        .result(response)
                        .build());
    }

    @PostMapping("/music")
    public Mono<ApiResponse<UserMusics>> selectMusic(@Valid @RequestBody MusicSelectRequest request) {
        return mediaForProfile.selectProfileMusic(request)
                .map(response -> ApiResponse.<UserMusics>builder()
                        .message("Profile music selected")
                        .result(response)
                        .build());
    }

    @GetMapping("/{userId}")
    public Mono<ApiResponse<PageResponse<Media>>> getUploadedMedia(
            @PathVariable String userId,
            @RequestParam OwnerType mode,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return mediaForProfile.getUploadedMedia(userId, mode, page, size)
                .map(response -> ApiResponse.<PageResponse<Media>>builder()
                        .message("Profile media fetched")
                        .result(response)
                        .build());
    }

    @GetMapping("/{userId}/avatar/current")
    public Mono<ApiResponse<Media>> getCurrentAvatar(
            @PathVariable String userId,
            @RequestParam(defaultValue = "AVATAR") MediaDisplayType mediaType
    ) {
        return mediaForProfile.getCurrentAvatar(userId, mediaType)
                .map(response -> ApiResponse.<Media>builder()
                        .message("Current avatar fetched")
                        .result(response)
                        .build());
    }

    @GetMapping("/{userId}/music/history")
    public Mono<ApiResponse<PageResponse<Media>>> getProfileMusicHistory(
            @PathVariable String userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return mediaForProfile.getProfileMusicHistory(userId, page, size)
                .map(response -> ApiResponse.<PageResponse<Media>>builder()
                        .message("Profile music history fetched")
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
        return mediaForProfile.getStories(userId, authentication.getName(), page, size, mediaType)
                .map(response -> ApiResponse.<PageResponse<UserStories>>builder()
                        .message("Stories fetched")
                        .result(response)
                        .build());
    }
}
