package com.dauducbach.clone.modules.media.infrastructure.cloudinary;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
@Slf4j
public class CloudinaryUrlTransformService {
    public String transformDeliveryUrl(String mediaUrl, MediaDisplayType displayType) {
        if (mediaUrl == null || mediaUrl.isBlank() || displayType == null) {
            return mediaUrl;
        }
        if (!CloudinaryUtils.isCloudinaryDeliveryUrl(mediaUrl)) {
            return mediaUrl.trim();
        }
        try {
            return CloudinaryUtils.withTransformations(mediaUrl, displayType.transformations());
        } catch (IllegalArgumentException error) {
            log.warn("|CloudinaryUrlTransformService|transformDeliveryUrl|fallback|displayType={}|error={}",
                    displayType, error.getMessage());
            return mediaUrl.trim();
        }
    }

    public String storyVideoStill(String mediaUrl, long previewAtMs) {
        if (mediaUrl == null || mediaUrl.isBlank() || previewAtMs < 0
                || !CloudinaryUtils.isCloudinaryDeliveryUrl(mediaUrl)) {
            return null;
        }
        try {
            String seconds = BigDecimal.valueOf(previewAtMs, 3)
                    .stripTrailingZeros()
                    .toPlainString();
            return CloudinaryUtils.withTransformations(mediaUrl, "so_" + seconds, "f_jpg", "q_auto");
        } catch (IllegalArgumentException error) {
            log.warn("|CloudinaryUrlTransformService|storyVideoStill|fallback|error={}", error.getMessage());
            return null;
        }
    }

    public void validateMusicSegment(Long musicStart, Long musicEnd) {
        CloudinaryUtils.validateAudioSegment(musicStart, musicEnd);
    }

    public String transformMusicUrl(String musicUrl, Long musicStart, Long musicEnd) {
        CloudinaryUtils.validateAudioSegment(musicStart, musicEnd);
        return CloudinaryUtils.withTransformations(
                musicUrl,
                "so_" + musicStart,
                "du_" + (musicEnd - musicStart));
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
