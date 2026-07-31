package com.dauducbach.clone.modules.media.service;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import com.dauducbach.clone.modules.media.entity.Media;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Stable application boundary for modules that need media metadata or delivery URLs.
 */
@Service
@RequiredArgsConstructor
public class MediaCompatibilityFacade {
    private final CloudinaryMediaService cloudinaryMediaService;
    private final MediaAssetCleanupService cleanupService;
    private final CloudinaryAudioStorageService audioStorageService;

    public Mono<Media> fetchMediaByPublicId(String publicId) {
        return cloudinaryMediaService.fetchMediaByPublicId(publicId);
    }

    public Flux<Media> fetchMediaList(List<String> publicIds, String ownerId, OwnerType ownerType) {
        return cloudinaryMediaService.fetchMediaList(publicIds, ownerId, ownerType);
    }

    public Mono<MediaAudioUploadResult> uploadMusic(byte[] bytes, String publicId) {
        return audioStorageService.uploadMusic(bytes, publicId);
    }

    public Mono<Void> deleteAsset(String publicId) {
        return cleanupService.delete(publicId);
    }

    public String transformDeliveryUrl(String mediaUrl, MediaDisplayType displayType) {
        return cloudinaryMediaService.transformDeliveryUrl(mediaUrl, displayType);
    }

    public String storyVideoStill(String mediaUrl, long previewAtMs) {
        return cloudinaryMediaService.storyVideoStill(mediaUrl, previewAtMs);
    }

    public void validateMusicSegment(Long musicStart, Long musicEnd) {
        CloudinaryUtils.validateAudioSegment(musicStart, musicEnd);
    }

    public String transformMusicUrl(String musicUrl, Long musicStart, Long musicEnd) {
        return cloudinaryMediaService.transformMusicUrl(musicUrl, musicStart, musicEnd);
    }

    public String transformMusicUrlIfSupported(String musicUrl, Long musicStart, Long musicEnd) {
        if (musicUrl == null || musicUrl.isBlank()) {
            return musicUrl;
        }
        if (!CloudinaryUtils.isCloudinaryDeliveryUrl(musicUrl)) {
            return musicUrl.trim();
        }
        return transformMusicUrl(musicUrl, musicStart, musicEnd);
    }
}
