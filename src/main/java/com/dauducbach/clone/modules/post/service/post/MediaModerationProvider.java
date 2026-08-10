package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.utils.MediaScanUtils;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Locale;

@Service
@RequiredArgsConstructor
public class MediaModerationProvider {
    private static final Logger log = LoggerFactory.getLogger(MediaModerationProvider.class);

    private final MediaScanUtils mediaScanUtils;
    private final MediaPolicyProperties mediaPolicy;

    public Mono<Decision> scan(String mediaUrl, String publicId) {
        return scan(mediaUrl, publicId, null);
    }

    public Mono<Decision> scan(String mediaUrl, String publicId, String declaredType) {
        if (resolveKind(declaredType, mediaUrl) == MediaKind.VIDEO) {
            log.info(
                    "|MediaModerationProvider|scan|videoBypassed|publicId={}|declaredType={}",
                    publicId, declaredType);
            return Mono.just(Decision.APPROVED);
        }
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
        long bytes = media.getBytes();
        return switch (resourceType) {
            case "image" -> bytes <= mediaPolicy.imageMaxBytes();
            case "video" -> bytes <= mediaPolicy.videoMaxBytes();
            default -> false;
        };
    }

    private MediaKind resolveKind(String declaredType, String mediaUrl) {
        String normalizedType = normalize(declaredType);
        if (normalizedType.contains("video")
                || normalizedType.matches(".*(mp4|webm|mov|m4v).*")) {
            return MediaKind.VIDEO;
        }
        if (normalizedType.contains("image")) {
            return MediaKind.IMAGE;
        }
        String normalizedUrl = normalize(mediaUrl);
        if (normalizedUrl.contains("/video/upload/")
                || hasVideoExtension(normalizedUrl)) {
            return MediaKind.VIDEO;
        }
        return MediaKind.IMAGE;
    }

    private boolean hasVideoExtension(String normalizedUrl) {
        int queryIndex = normalizedUrl.indexOf('?');
        String path = queryIndex >= 0 ? normalizedUrl.substring(0, queryIndex) : normalizedUrl;
        return path.endsWith(".mp4")
                || path.endsWith(".webm")
                || path.endsWith(".mov")
                || path.endsWith(".m4v");
    }

    private String normalize(String value) {
        return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    }

    private enum MediaKind {
        IMAGE,
        VIDEO
    }

    public enum Decision {
        APPROVED,
        REJECTED
    }
}
