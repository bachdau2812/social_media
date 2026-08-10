package com.dauducbach.clone.modules.media.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import com.dauducbach.clone.modules.media.dto.response.MediaUploadPolicyResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/media")
public class MediaPolicyController {
    private final MediaPolicyProperties properties;

    @GetMapping("/upload-policy")
    public Mono<ApiResponse<MediaUploadPolicyResponse>> getUploadPolicy() {
        MediaUploadPolicyResponse policy = new MediaUploadPolicyResponse(
                properties.imageMaxBytes(),
                properties.videoMaxBytes(),
                properties.audioMaxBytes());
        return Mono.just(ApiResponse.<MediaUploadPolicyResponse>builder()
                .message("OK")
                .result(policy)
                .build());
    }
}
