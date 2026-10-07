package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.user.dto.request.AvatarUploadRequest;
import com.dauducbach.clone.modules.user.dto.request.MusicSelectRequest;
import com.dauducbach.clone.modules.user.dto.response.ProfileMediaUploadResponse;
import com.dauducbach.clone.modules.user.entity.UserMusics;
import com.dauducbach.clone.modules.user.profile.application.AvatarUploadUseCase;
import com.dauducbach.clone.modules.user.profile.application.ProfileMediaQuery;
import com.dauducbach.clone.modules.user.profile.application.ProfileMusicUseCase;
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
@RequestMapping("/profile-media")
public class MediaForProfileController {
    private final AvatarUploadUseCase avatarUploadUseCase;
    private final ProfileMusicUseCase profileMusicUseCase;
    private final ProfileMediaQuery profileMediaQuery;

    @PostMapping("/avatar")
    public Mono<ApiResponse<ProfileMediaUploadResponse>> uploadAvatar(@Valid @RequestBody AvatarUploadRequest request) {
        return avatarUploadUseCase.uploadAvatar(request)
                .map(response -> ApiResponse.<ProfileMediaUploadResponse>builder()
                        .message("Avatar upload accepted")
                        .result(response)
                        .build());
    }

    @PostMapping("/music")
    public Mono<ApiResponse<UserMusics>> selectMusic(@Valid @RequestBody MusicSelectRequest request) {
        return profileMusicUseCase.selectProfileMusic(request)
                .map(response -> ApiResponse.<UserMusics>builder()
                        .message("Profile music selected")
                        .result(response)
                        .build());
    }

    @GetMapping("/{userId}")
    public Mono<ApiResponse<PageResponse<MediaAssetView>>> getUploadedMedia(
            @PathVariable String userId,
            @RequestParam OwnerType mode,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return profileMediaQuery.getUploadedMedia(userId, mode, page, size)
                .map(response -> ApiResponse.<PageResponse<MediaAssetView>>builder()
                        .message("Profile media fetched")
                        .result(response)
                        .build());
    }

    @GetMapping("/{userId}/avatar/current")
    public Mono<ApiResponse<MediaAssetView>> getCurrentAvatar(
            @PathVariable String userId,
            @RequestParam(defaultValue = "AVATAR") MediaDisplayType mediaType
    ) {
        return profileMediaQuery.getCurrentAvatar(userId, mediaType)
                .map(response -> ApiResponse.<MediaAssetView>builder()
                        .message("Current avatar fetched")
                        .result(response)
                        .build());
    }

    @GetMapping("/{userId}/music/history")
    public Mono<ApiResponse<PageResponse<MediaAssetView>>> getProfileMusicHistory(
            @PathVariable String userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return profileMediaQuery.getProfileMusicHistory(userId, page, size)
                .map(response -> ApiResponse.<PageResponse<MediaAssetView>>builder()
                        .message("Profile music history fetched")
                        .result(response)
                        .build());
    }
}
