package com.dauducbach.clone.modules.post.service;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.constant.OwnerType;
import com.dauducbach.clone.modules.post.entity.Media;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
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
                    Map<String, Object> result = cloudinary.api().resource(publicId, ObjectUtils.emptyMap());

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