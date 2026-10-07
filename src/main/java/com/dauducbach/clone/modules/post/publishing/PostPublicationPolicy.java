package com.dauducbach.clone.modules.post.publishing;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.constant.PostMediaRatio;
import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.post.dto.request.PostCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostItemCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostUpdateRequest;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import org.jsoup.Jsoup;
import org.jsoup.safety.Safelist;

import java.util.Comparator;
import java.util.List;
import java.time.Instant;
import java.util.stream.IntStream;

/** Post creation and update rules that do not need database or infrastructure access. */
public final class PostPublicationPolicy {
    private PostPublicationPolicy() { }

    public static void validateCreateRequest(PostCreateRequest request) {
        if (request == null) {
            throw new AppException(ErrorCode.POST_CREATE_FAILED, "Post request is required");
        }
        normalizeRequired(request.getUserId(), "userId is required");
        if (normalizeOptional(request.getMediaRatio()) != null && !PostMediaRatio.isSupported(request.getMediaRatio())) {
            throw new AppException(ErrorCode.POST_CREATE_FAILED,
                    "mediaRatio must be one of 1:1, 4:5, 3:4, 9:16, 4:3, 3:2, 16:9");
        }

        List<PostItemCreateRequest> items = request.getItems() == null ? List.of() : request.getItems();
        if (items.isEmpty() && normalizeOptional(request.getContent()) == null) {
            throw new AppException(ErrorCode.POST_CONTENT_INVALID, "Post content or media is required");
        }

        String commonMusicId = normalizeOptional(request.getMusicId());
        if (commonMusicId != null) {
            validateMusicSegment(commonMusicId, request.getMusicStart(), request.getMusicEnd(), "post");
        }
        for (int index = 0; index < items.size(); index++) {
            PostItemCreateRequest item = items.get(index);
            int orderNumber = item.getOrderNumber() == null || item.getOrderNumber() <= 0
                    ? index + 1 : item.getOrderNumber();
            normalizeRequired(item.getSecureUrl(), "secureUrl is required for post item " + orderNumber);
            normalizeRequired(item.getPublicId(), "publicId is required for post item " + orderNumber);
            if (commonMusicId == null) {
                String itemMusicId = normalizeOptional(item.getMusicId());
                if (itemMusicId != null && !isVideoItem(item)) {
                    validateMusicSegment(itemMusicId, item.getMusicStart(), item.getMusicEnd(), "post item " + orderNumber);
                }
            }
        }
    }

    public static List<PostMediaScanItem> buildScanItems(PostCreateRequest request) {
        List<PostItemCreateRequest> items = request.getItems() == null ? List.of() : request.getItems();
        boolean useSharedMusic = normalizeOptional(request.getMusicId()) != null;
        return IntStream.range(0, items.size())
                .mapToObj(index -> {
                    PostItemCreateRequest item = items.get(index);
                    int orderNumber = item.getOrderNumber() == null || item.getOrderNumber() <= 0
                            ? index + 1 : item.getOrderNumber();
                    boolean videoItem = isVideoItem(item);
                    String itemMusicId = useSharedMusic || videoItem ? null : normalizeOptional(item.getMusicId());
                    return PostMediaScanItem.builder()
                            .orderNumber(orderNumber)
                            .secureUrl(normalizeRequired(item.getSecureUrl(), "secureUrl is required for post item " + orderNumber))
                            .publicId(normalizeRequired(item.getPublicId(), "publicId is required for post item " + orderNumber))
                            .resourceType(normalizeOptional(item.getResourceType()))
                            .caption(normalizeOptional(item.getCaption()))
                            .musicId(itemMusicId)
                            .musicStart(itemMusicId == null ? null : item.getMusicStart())
                            .musicEnd(itemMusicId == null ? null : item.getMusicEnd())
                            .build();
                })
                .sorted(Comparator.comparing(PostMediaScanItem::getOrderNumber))
                .toList();
    }

    public static void applyMetadataUpdate(PostDetails existing, PostUpdateRequest request, boolean hasExistingMedia) {
        if (request.getContent() != null) {
            existing.setContent(sanitizeContent(request.getContent(), hasExistingMedia || request.getItems() != null));
        }
        if (request.getHashtag() != null) {
            existing.setHashtagList(request.getHashtag());
        }
        if (request.getMediaRatio() != null) {
            if (!PostMediaRatio.isSupported(request.getMediaRatio())) {
                throw new AppException(ErrorCode.POST_UPDATE_FAILED, "Unsupported mediaRatio");
            }
            existing.setMediaRatio(PostMediaRatio.defaultIfMissing(request.getMediaRatio()));
        }
        if (request.getMusicId() != null || request.getMusicStart() != null || request.getMusicEnd() != null) {
            String musicId = normalizeOptional(request.getMusicId());
            validateMusicSegment(musicId, request.getMusicStart(), request.getMusicEnd(), "post");
            existing.setMusicId(musicId);
            existing.setMusicStart(musicId == null ? null : request.getMusicStart());
            existing.setMusicEnd(musicId == null ? null : request.getMusicEnd());
        }
        existing.setUpdatedAt(Instant.now());
    }

    public static String sanitizeContent(String content, boolean allowEmpty) {
        String normalized = normalizeOptional(content);
        if (normalized == null) {
            if (allowEmpty) return "";
            throw new AppException(ErrorCode.POST_CONTENT_INVALID, "Post content is empty");
        }

        String sanitized = Jsoup.clean(normalized, Safelist.relaxed()).trim();
        if (sanitized.isBlank() && !allowEmpty) {
            throw new AppException(ErrorCode.POST_CONTENT_INVALID, "Post content is invalid after sanitization");
        }
        return sanitized;
    }

    public static String normalizeRatio(String ratio) {
        return PostMediaRatio.defaultIfMissing(ratio);
    }

    public static String normalizeRequired(String value, String message) {
        String normalized = normalizeOptional(value);
        if (normalized == null) throw new AppException(ErrorCode.POST_CREATE_FAILED, message);
        return normalized;
    }

    public static String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private static boolean isVideoItem(PostItemCreateRequest item) {
        String value = firstNonBlank(item.getResourceType(), item.getSecureUrl());
        if (value == null) return false;
        String lower = value.toLowerCase();
        return lower.contains("video") || lower.endsWith(".mp4") || lower.endsWith(".mov")
                || lower.endsWith(".webm") || lower.endsWith(".m4v");
    }

    private static String firstNonBlank(String first, String second) {
        String normalizedFirst = normalizeOptional(first);
        return normalizedFirst != null ? normalizedFirst : normalizeOptional(second);
    }

    public static void validateMusicSegment(String musicId, Long musicStart, Long musicEnd, String scope) {
        if (musicId == null) {
            if (musicStart != null || musicEnd != null) {
                throw new AppException(ErrorCode.POST_CREATE_FAILED,
                        "musicId is required when a music segment is provided for " + scope);
            }
            return;
        }
        if (musicStart == null || musicEnd == null) {
            throw new AppException(ErrorCode.POST_CREATE_FAILED,
                    "musicStart and musicEnd are required for " + scope);
        }
        if (musicStart < 0 || musicEnd <= musicStart) {
            throw new AppException(ErrorCode.POST_CREATE_FAILED, "Invalid music segment for " + scope);
        }
    }
}
