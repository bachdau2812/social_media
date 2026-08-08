package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.utils.MediaScanUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class MediaModerationProvider {
    private static final int IMAGE_MAX_BYTES = 50 * 1024 * 1024;
    private static final int VIDEO_MAX_BYTES = 500 * 1024 * 1024;

    private final MediaScanUtils mediaScanUtils;

    public Mono<Decision> scan(String mediaUrl, String publicId) {
        return mediaScanUtils.scanMedia(mediaUrl, publicId)
                .map(result -> result.isNsfw() ? Decision.REJECTED : Decision.APPROVED);
    }

    public boolean isAllowedAsset(Media media) {
        if (media == null) {
            return false;
        }
        String resourceType = media.getResourceType() == null
                ? ""
                : media.getResourceType().trim().toLowerCase();
        int bytes = media.getBytes();
        return switch (resourceType) {
            case "image" -> bytes <= IMAGE_MAX_BYTES;
            case "video" -> bytes <= VIDEO_MAX_BYTES;
            default -> false;
        };
    }

    public enum Decision {
        APPROVED,
        REJECTED
    }
}
