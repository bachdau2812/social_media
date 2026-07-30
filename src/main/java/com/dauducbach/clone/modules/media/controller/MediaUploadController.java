package com.dauducbach.clone.modules.media.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.dto.response.MediaSignatureResponse;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.service.CloudinarySignatureService;
import com.dauducbach.clone.modules.media.service.MediaService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/media")
public class MediaUploadController {
    private final CloudinarySignatureService cloudinarySignatureService;
    private final MediaService mediaService;

    @GetMapping("/signature")
    public Mono<ApiResponse<MediaSignatureResponse>> getUploadSignature() {
        return Mono.fromSupplier(cloudinarySignatureService::generateSignature)
                .map(response -> ApiResponse.<MediaSignatureResponse>builder()
                        .message("OK")
                        .result(response)
                        .build());
    }

    @GetMapping("/public/{publicId}")
    public Mono<ApiResponse<Media>> getMediaByPublicId(@PathVariable String publicId) {
        return mediaService.getByPublicId(publicId)
                .map(media -> ApiResponse.<Media>builder()
                        .message("OK")
                        .result(media)
                        .build());
    }

    @GetMapping("/owner/{ownerId}")
    public Flux<Media> getMediaByOwnerId(@PathVariable String ownerId,
                                         @RequestParam(required = false) OwnerType ownerType) {
        return mediaService.getByOwnerId(ownerId, ownerType);
    }
}
