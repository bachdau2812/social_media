package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.constant.OwnerType;
import com.dauducbach.clone.modules.post.entity.Media;
import com.dauducbach.clone.modules.post.repositoty.MediaRepository;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
public class MediaService {
    MediaRepository mediaRepository;
    CloudinaryMediaService cloudinaryMediaService;
    R2dbcEntityTemplate r2dbcEntityTemplate;

    public Mono<Media> getByPublicId(String publicId) {
        if (publicId == null || publicId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.MEDIA_FETCH_FAILED, "publicId is required"));
        }

        log.info("|MediaService|getByPublicId|start|publicId={}", publicId);
        return mediaRepository.findByPublicId(publicId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.MEDIA_NOT_FOUND,
                        String.format("Media not found for publicId=%s", publicId)
                )))
                .doOnNext(media -> log.info("|MediaService|getByPublicId|success|publicId={}|assetId={}",
                        publicId, media.getAssetId()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.MEDIA_FETCH_FAILED,
                                String.format("Fetch media failed for publicId=%s", publicId),
                                error
                        ));
    }

    public Flux<Media> getByOwnerId(String ownerId, OwnerType ownerType) {
        if (ownerId == null || ownerId.isBlank()) {
            return Flux.error(new AppException(ErrorCode.MEDIA_FETCH_FAILED, "ownerId is required"));
        }

        log.info("|MediaService|getByOwnerId|start|ownerId={}|ownerType={}", ownerId, ownerType);
        Flux<Media> mediaFlux = ownerType == null
                ? mediaRepository.findByOwnerId(ownerId)
                : mediaRepository.findByOwnerIdAndOwnerType(ownerId, ownerType);

        return mediaFlux
                .doOnComplete(() -> log.info("|MediaService|getByOwnerId|completed|ownerId={}|ownerType={}",
                        ownerId, ownerType))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.MEDIA_FETCH_FAILED,
                                String.format("Fetch media failed for ownerId=%s", ownerId),
                                error
                        ));
    }

    public Mono<Media> saveCloudinaryMedia(String publicId, String ownerId, OwnerType ownerType) {
        if (publicId == null || publicId.isBlank() || ownerId == null || ownerId.isBlank() || ownerType == null) {
            return Mono.error(new AppException(ErrorCode.MEDIA_SAVE_FAILED, "publicId, ownerId and ownerType are required"));
        }

        return cloudinaryMediaService.fetchMediaByPublicId(publicId.trim())
                .doOnNext(media -> {
                    media.setOwnerId(ownerId.trim());
                    media.setOwnerType(ownerType);
                    media.setUpdatedAt(Instant.now());
                })
                .flatMap(media -> r2dbcEntityTemplate.insert(Media.class).using(media))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.MEDIA_SAVE_FAILED,
                                String.format("Save media failed for ownerId=%s ownerType=%s", ownerId, ownerType),
                                error
                        ));
    }

    public Mono<Media> saveFeatureMusic(String userId, String musicId, String displayName, String slugName, String songUrl, String displayImages) {
        if (userId == null || userId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.MEDIA_SAVE_FAILED, "userId is required"));
        }

        Instant now = Instant.now();
        Media media = Media.builder()
                .assetId(UUID.randomUUID().toString())
                .publicId(slugName)
                .mediaFormat("audio")
                .resourceType("audio")
                .url(songUrl)
                .secureUrl(songUrl)
                .ownerId(userId)
                .ownerType(OwnerType.FEATURE_MUSIC)
                .displayName(displayName)
                .version(musicId)
                .versionId(displayImages)
                .createdAt(now)
                .updatedAt(now)
                .build();

        return r2dbcEntityTemplate.insert(Media.class).using(media)
                .onErrorMap(error -> new AppException(
                        ErrorCode.MEDIA_SAVE_FAILED,
                        String.format("Save feature music media failed for userId=%s", userId),
                        error
                ));
    }

    public Mono<Media> saveImportedMusicMedia(String musicId, String displayName, Map<String, Object> uploadResult) {
        if (musicId == null || musicId.isBlank() || uploadResult == null || uploadResult.isEmpty()) {
            return Mono.error(new AppException(ErrorCode.MEDIA_SAVE_FAILED, "musicId and uploadResult are required"));
        }

        Instant now = Instant.now();
        Media media = Media.builder()
                .assetId(stringValue(uploadResult.get("asset_id"), UUID.randomUUID().toString()))
                .publicId(stringValue(uploadResult.get("public_id"), musicId))
                .width(intValue(uploadResult.get("width")))
                .height(intValue(uploadResult.get("height")))
                .mediaFormat(stringValue(uploadResult.get("format"), "mp3"))
                .resourceType(stringValue(uploadResult.get("resource_type"), "video"))
                .bytes(intValue(uploadResult.get("bytes")))
                .url(stringValue(uploadResult.get("url"), null))
                .secureUrl(stringValue(uploadResult.get("secure_url"), null))
                .ownerId(musicId)
                .ownerType(OwnerType.MUSIC)
                .displayName(displayName)
                .version(stringValue(uploadResult.get("version"), null))
                .versionId(stringValue(uploadResult.get("version_id"), null))
                .createdAt(now)
                .updatedAt(now)
                .build();

        return r2dbcEntityTemplate.insert(Media.class).using(media)
                .onErrorMap(error -> new AppException(
                        ErrorCode.MEDIA_SAVE_FAILED,
                        String.format("Save imported music media failed for musicId=%s", musicId),
                        error
                ));
    }

    public Mono<PageResponse<Media>> getProfileMedia(String userId, OwnerType ownerType, int page, int size) {
        if (userId == null || userId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.MEDIA_FETCH_FAILED, "userId is required"));
        }
        if (ownerType == null) {
            return Mono.error(new AppException(ErrorCode.MEDIA_FETCH_FAILED, "ownerType is required"));
        }

        int pageNumber = Math.max(page, 0);
        int pageSize = Math.min(Math.max(size, 1), 100);
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        Mono<Long> countMono = OwnerType.POST.equals(ownerType)
                ? mediaRepository.countPostMediaByUserId(userId)
                : mediaRepository.countByOwnerIdAndOwnerType(userId, ownerType);

        Flux<Media> mediaFlux = OwnerType.POST.equals(ownerType)
                ? mediaRepository.findPostMediaByUserId(userId, pageable)
                : mediaRepository.findByOwnerIdAndOwnerTypeOrderByCreatedAtDesc(userId, ownerType, pageable);

        return countMono.flatMap(total -> mediaFlux.collectList()
                        .map(items -> PageResponse.of(items, pageNumber, total, pageSize)))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.MEDIA_FETCH_FAILED,
                                String.format("Fetch profile media failed for userId=%s ownerType=%s", userId, ownerType),
                                error
                        ));
    }

    public Mono<Media> getCurrentAvatar(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.MEDIA_FETCH_FAILED, "userId is required"));
        }

        return mediaRepository.findFirstByOwnerIdAndOwnerTypeOrderByCreatedAtDesc(userId, OwnerType.AVATAR)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.MEDIA_NOT_FOUND,
                        String.format("Current avatar not found for userId=%s", userId)
                )));
    }

    public Mono<List<Media>> saveCloudinaryMediaList(List<String> publicIds, String ownerId, OwnerType ownerType) {
        return Flux.fromIterable(publicIds == null ? List.<String>of() : publicIds)
                .filter(publicId -> publicId != null && !publicId.isBlank())
                .concatMap(publicId -> saveCloudinaryMedia(publicId, ownerId, ownerType))
                .collectList();
    }

    private String stringValue(Object value, String fallback) {
        return value == null ? fallback : String.valueOf(value);
    }

    private int intValue(Object value) {
        return value instanceof Number number ? number.intValue() : 0;
    }
}
