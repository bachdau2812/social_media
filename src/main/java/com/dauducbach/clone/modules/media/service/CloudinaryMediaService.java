package com.dauducbach.clone.modules.media.service;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;

import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Service for interacting with Cloudinary API to retrieve detailed media information.
 * Handles fetching media metadata by public ID and converting to Media entity.
 */
@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
public class CloudinaryMediaService {

    Cloudinary cloudinary;

    /**
     * Fetch detailed media information from Cloudinary by public ID.
     * Retrieves complete metadata including dimensions, format, URLs, and timestamps.
     *
     * @param publicId the Cloudinary public ID
     * @return Mono of Media entity with complete metadata
     */
    public Mono<Media> fetchMediaByPublicId(String publicId) {
        return Mono.fromCallable(() -> {
                try {
                    log.info("|CloudinaryMediaService|fetchMediaByPublicId|start|publicId={}", publicId);

                    @SuppressWarnings("unchecked")
                    Map<String, Object> result = fetchCloudinaryResource(publicId);

                    log.info("|CloudinaryMediaService|fetchMediaByPublicId|success|publicId={}|assetId={}",
                            publicId, result.get("asset_id"));

                    return result;
                } catch (Exception e) {
                    log.error("|CloudinaryMediaService|fetchMediaByPublicId|failed|publicId={}|error={}",
                            publicId, e.getMessage());
                    throw e;
                }
            })
            .subscribeOn(Schedulers.boundedElastic())
            .map(this::mapToMediaEntity)
            .onErrorMap(e -> new AppException(
                    ErrorCode.CLOUDINARY_API_ERROR,
                    String.format("Failed to fetch media details from Cloudinary for publicId=%s", publicId),
                    e
            ));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fetchCloudinaryResource(String publicId) throws Exception {
        try {
            return cloudinary.api().resource(publicId, ObjectUtils.emptyMap());
        } catch (Exception imageError) {
            log.warn("|CloudinaryMediaService|fetchCloudinaryResource|image fetch failed, retry video|publicId={}|error={}",
                    publicId, imageError.getMessage());
            return cloudinary.api().resource(publicId, ObjectUtils.asMap("resource_type", "video"));
        }
    }
    /**
     * Fetch media details for multiple public IDs.
     * Useful for batch processing when multiple media need to be stored.
     *
     * @param publicIds list of Cloudinary public IDs
     * @param ownerId the owner ID (post ID or comment ID)
     * @param ownerType the owner type (POST or COMMENT)
     * @return Flux of Media entities with complete metadata
     */
    public Flux<Media> fetchMediaList(List<String> publicIds, String ownerId, OwnerType ownerType) {
        return Flux.fromIterable(publicIds)
                .flatMap(publicId -> fetchMediaByPublicId(publicId)
                        .doOnNext(media -> {
                            media.setOwnerId(ownerId);
                            media.setOwnerType(ownerType);
                            media.setUpdatedAt(Instant.now());
                            log.info("|CloudinaryMediaService|fetchMediaList|media mapped|publicId={}|ownerId={}|ownerType={}",
                                    publicId, ownerId, ownerType);
                        })
                        .onErrorResume(e -> {
                            log.error("|CloudinaryMediaService|fetchMediaList|skip failed media|publicId={}|error={}",
                                    publicId, e.getMessage());
                            return Mono.empty();
                        })
                );
    }

    /**
     * Map Cloudinary API response to Media entity.
     * Extracts and converts all relevant fields from Cloudinary response.
     *
     * @param result the Cloudinary API response map
     * @return Media entity with populated fields
     */
    /**
     * Builds a transformed Cloudinary delivery URL without mutating the stored media entity.
     * MUSIC currently supports segment clipping; IMAGE and VIDEO accept extension directives.
     */
    public Mono<MediaTransformationResult> transformMedia(Media media, MediaTransformRequest request) {
        if (media == null) {
            return Mono.error(new AppException(
                    ErrorCode.CLOUDINARY_API_ERROR,
                    "Media is required for transformation"
            ));
        }
        String sourceUrl = firstNonBlank(media.getSecureUrl(), media.getUrl());
        return transformMediaUrl(sourceUrl, request);
    }

    public Mono<MediaTransformationResult> transformMediaUrl(String mediaUrl, MediaTransformRequest request) {
        return Mono.fromCallable(() -> {
                    String transformedUrl = buildTransformedUrl(mediaUrl, request);
                    return new MediaTransformationResult(
                            mediaUrl.trim(),
                            transformedUrl,
                            request.type(),
                            buildTransformationChain(request)
                    );
                })
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSuccess(result -> log.info(
                        "|CloudinaryMediaService|transformMediaUrl|success|type={}|directives={}",
                        result.type(), result.transformations().size()
                ))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.CLOUDINARY_API_ERROR,
                                "Failed to apply Cloudinary media transformation",
                                error
                        ));
    }

    public String transformMusicUrl(String musicUrl, Long musicStart, Long musicEnd) {
        return buildTransformedUrl(
                musicUrl,
                MediaTransformRequest.musicSegment(musicStart, musicEnd)
        );
    }

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
            log.warn("|CloudinaryMediaService|transformDeliveryUrl|fallback|displayType={}|error={}",
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
            String seconds = java.math.BigDecimal.valueOf(previewAtMs, 3)
                    .stripTrailingZeros()
                    .toPlainString();
            return CloudinaryUtils.withTransformations(
                    mediaUrl, "so_" + seconds, "f_jpg", "q_auto");
        } catch (IllegalArgumentException error) {
            log.warn("|CloudinaryMediaService|storyVideoStill|fallback|error={}", error.getMessage());
            return null;
        }
    }

    public String buildTransformedUrl(String mediaUrl, MediaTransformRequest request) {
        if (request == null) {
            throw new AppException(ErrorCode.CLOUDINARY_API_ERROR, "Media transform request is required");
        }
        List<String> transformations = buildTransformationChain(request);
        return CloudinaryUtils.withTransformations(mediaUrl, transformations);
    }

    private List<String> buildTransformationChain(MediaTransformRequest request) {
        List<String> transformations = new ArrayList<>();
        if (request.type() == MediaTransformType.MUSIC) {
            CloudinaryUtils.validateAudioSegment(request.start(), request.end());
            transformations.add("so_" + request.start());
            transformations.add("du_" + (request.end() - request.start()));
        }
        request.transformations().stream()
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .peek(this::validateTransformationDirective)
                .forEach(transformations::add);
        if (transformations.isEmpty()) {
            throw new AppException(
                    ErrorCode.CLOUDINARY_API_ERROR,
                    "At least one Cloudinary transformation is required"
            );
        }
        return List.copyOf(transformations);
    }

    private void validateTransformationDirective(String directive) {
        if (directive.contains("/") || directive.contains("..")) {
            throw new AppException(
                    ErrorCode.CLOUDINARY_API_ERROR,
                    "Cloudinary transformation directive is invalid"
            );
        }
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first.trim();
        }
        return second == null || second.isBlank() ? null : second.trim();
    }

    public enum MediaTransformType {
        MUSIC,
        IMAGE,
        VIDEO
    }

    public record MediaTransformRequest(
            MediaTransformType type,
            Long start,
            Long end,
            List<String> transformations
    ) {
        public MediaTransformRequest {
            if (type == null) {
                throw new IllegalArgumentException("Media transform type is required");
            }
            transformations = transformations == null ? List.of() : List.copyOf(transformations);
        }

        public static MediaTransformRequest musicSegment(Long start, Long end) {
            return new MediaTransformRequest(MediaTransformType.MUSIC, start, end, List.of());
        }

        public static MediaTransformRequest image(List<String> transformations) {
            return new MediaTransformRequest(MediaTransformType.IMAGE, null, null, transformations);
        }

        public static MediaTransformRequest video(List<String> transformations) {
            return new MediaTransformRequest(MediaTransformType.VIDEO, null, null, transformations);
        }
    }

    public record MediaTransformationResult(
            String sourceUrl,
            String transformedUrl,
            MediaTransformType type,
            List<String> transformations
    ) {
        public MediaTransformationResult {
            transformations = transformations == null ? List.of() : List.copyOf(transformations);
        }
    }
    private Media mapToMediaEntity(Map<String, Object> result) {
        log.info("|CloudinaryMediaService|mapToMediaEntity|start|publicId={}", result.get("public_id"));

        Media.MediaBuilder builder = Media.builder();

        // Asset ID - Primary key from Cloudinary
        builder.assetId((String) result.get("asset_id"));

        // Public ID - Cloudinary public identifier
        builder.publicId((String) result.get("public_id"));

        // Dimensions - Width and Height
        Object width = result.get("width");
        Object height = result.get("height");
        builder.width(width != null ? ((Number) width).intValue() : 0);
        builder.height(height != null ? ((Number) height).intValue() : 0);

        // Media Format - jpg, mp4, etc.
        builder.mediaFormat((String) result.get("format"));

        // Resource Type - image, video, etc.
        builder.resourceType((String) result.get("resource_type"));

        // File Size - Bytes
        Object bytes = result.get("bytes");
        builder.bytes(bytes != null ? ((Number) bytes).intValue() : 0);

        // URLs
        builder.url((String) result.get("url"));
        builder.secureUrl((String) result.get("secure_url"));

        // Version information
        Object version = result.get("version");
        builder.version(version != null ? String.valueOf(version) : null);

        builder.versionId((String) result.get("version_id"));

        // Created At - Parse ISO-8601 string to Instant
        String createdAtStr = (String) result.get("created_at");
        if (createdAtStr != null) {
            try {
                Instant createdAt = Instant.parse(createdAtStr);
                builder.createdAt(createdAt);
                log.debug("|CloudinaryMediaService|mapToMediaEntity|parsed createdAt|publicId={}|createdAt={}",
                        result.get("public_id"), createdAt);
            } catch (Exception e) {
                log.warn("|CloudinaryMediaService|mapToMediaEntity|invalid createdAt format|publicId={}|createdAtStr={}|error={}",
                        result.get("public_id"), createdAtStr, e.getMessage());
                builder.createdAt(Instant.now());
            }
        } else {
            builder.createdAt(Instant.now());
        }

        // Updated At - Default to current time
        builder.updatedAt(Instant.now());

        Media media = builder.build();
        log.info("|CloudinaryMediaService|mapToMediaEntity|completed|publicId={}|assetId={}|resourceType={}",
                media.getPublicId(), media.getAssetId(), media.getResourceType());

        return media;
    }
}
