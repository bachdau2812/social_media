package com.dauducbach.clone.modules.media.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.repository.MediaRepository;
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

    public Mono<Media> getById(String assetId) {
        if (assetId == null || assetId.isBlank()) {
            return Mono.empty();
        }
        return mediaRepository.findById(assetId.trim());
    }

    public Mono<Media> getFirstByOwnerIdAndOwnerType(String ownerId, OwnerType ownerType) {
        if (ownerId == null || ownerId.isBlank() || ownerType == null) {
            return Mono.empty();
        }
        return mediaRepository.findFirstByOwnerIdAndOwnerTypeOrderByCreatedAtAsc(
                ownerId.trim(), ownerType);
    }

    public Mono<Void> deleteByOwnerIdAndOwnerType(String ownerId, OwnerType ownerType) {
        if (ownerId == null || ownerId.isBlank() || ownerType == null) {
            return Mono.empty();
        }
        return mediaRepository.deleteByOwnerIdAndOwnerType(ownerId.trim(), ownerType);
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

        log.info("|MediaService|saveCloudinaryMedia|start|publicId={}|ownerId={}|ownerType={}",
                publicId, ownerId, ownerType);
        String cleanPublicId = publicId.trim();
        String cleanOwnerId = ownerId.trim();
        return mediaRepository.findByPublicId(cleanPublicId)
                .flatMap(existing -> {
                    if (cleanOwnerId.equals(existing.getOwnerId()) && ownerType == existing.getOwnerType()) {
                        log.info("|MediaService|saveCloudinaryMedia|idempotent hit|publicId={}|ownerId={}|ownerType={}",
                                cleanPublicId, cleanOwnerId, ownerType);
                        return Mono.just(existing);
                    }
                    return Mono.error(new AppException(
                            ErrorCode.MEDIA_SAVE_FAILED,
                            String.format("Media publicId=%s already belongs to ownerId=%s ownerType=%s",
                                    cleanPublicId, existing.getOwnerId(), existing.getOwnerType())
                    ));
                })
                .switchIfEmpty(Mono.defer(() -> cloudinaryMediaService.fetchMediaByPublicId(cleanPublicId)
                        .doOnNext(media -> {
                            media.setOwnerId(cleanOwnerId);
                            media.setOwnerType(ownerType);
                            media.setUpdatedAt(Instant.now());
                        })
                        .flatMap(media -> r2dbcEntityTemplate.insert(Media.class).using(media))))
                .doOnSuccess(saved -> log.info("|MediaService|saveCloudinaryMedia|saved|publicId={}|assetId={}|ownerId={}|ownerType={}",
                        publicId, saved.getAssetId(), ownerId, ownerType))
                .doOnError(error -> log.error("|MediaService|saveCloudinaryMedia|failed|publicId={}|ownerId={}|ownerType={}|error={}",
                        publicId, ownerId, ownerType, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.MEDIA_SAVE_FAILED,
                                String.format("Save media failed for ownerId=%s ownerType=%s", ownerId, ownerType),
                                error
                        ));
    }

    public Mono<Media> registerFetchedMedia(Media media, String ownerId, OwnerType ownerType) {
        if (media == null
                || media.getPublicId() == null
                || media.getPublicId().isBlank()
                || ownerId == null
                || ownerId.isBlank()
                || ownerType == null) {
            return Mono.error(new AppException(
                    ErrorCode.MEDIA_SAVE_FAILED,
                    "media, publicId, ownerId and ownerType are required"));
        }

        String publicId = media.getPublicId().trim();
        String cleanOwnerId = ownerId.trim();
        return mediaRepository.findByPublicId(publicId)
                .flatMap(existing -> {
                    if (cleanOwnerId.equals(existing.getOwnerId())
                            && ownerType == existing.getOwnerType()) {
                        return Mono.just(existing);
                    }
                    return Mono.error(new AppException(
                            ErrorCode.MEDIA_SAVE_FAILED,
                            String.format(
                                    "Media publicId=%s already belongs to ownerId=%s ownerType=%s",
                                    publicId, existing.getOwnerId(), existing.getOwnerType())
                    ));
                })
                .switchIfEmpty(Mono.defer(() -> {
                    Instant now = Instant.now();
                    media.setOwnerId(cleanOwnerId);
                    media.setOwnerType(ownerType);
                    if (media.getCreatedAt() == null) {
                        media.setCreatedAt(now);
                    }
                    media.setUpdatedAt(now);
                    return r2dbcEntityTemplate.insert(Media.class).using(media);
                }))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.MEDIA_SAVE_FAILED,
                                String.format(
                                        "Register media failed for ownerId=%s ownerType=%s",
                                        cleanOwnerId, ownerType),
                                error
                        ));
    }

    public Mono<Media> saveFeatureMusic(String userId, String musicId, String displayName, String slugName, String songUrl, String displayImages) {
        if (userId == null || userId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.MEDIA_SAVE_FAILED, "userId is required"));
        }

        log.info("|MediaService|saveFeatureMusic|start|userId={}|musicId={}|slugName={}", userId, musicId, slugName);
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
                .doOnSuccess(saved -> log.info("|MediaService|saveFeatureMusic|saved|userId={}|musicId={}|assetId={}",
                        userId, musicId, saved.getAssetId()))
                .doOnError(error -> log.error("|MediaService|saveFeatureMusic|failed|userId={}|musicId={}|error={}",
                        userId, musicId, error.getMessage()))
                .onErrorMap(error -> new AppException(
                        ErrorCode.MEDIA_SAVE_FAILED,
                        String.format("Save feature music media failed for userId=%s", userId),
                        error
                ));
    }

    public Mono<Media> saveImportedMusicMedia(
            String musicId,
            String displayName,
            com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult uploadResult
    ) {
        if (musicId == null || musicId.isBlank() || uploadResult == null) {
            return Mono.error(new AppException(
                    ErrorCode.MEDIA_SAVE_FAILED,
                    "musicId and uploadResult are required"));
        }

        log.info("|MediaService|saveImportedMusicMedia|start|musicId={}", musicId);
        Instant now = Instant.now();
        Media media = Media.builder()
                .assetId(uploadResult.assetId() == null
                        ? UUID.randomUUID().toString()
                        : uploadResult.assetId())
                .publicId(uploadResult.publicId() == null
                        ? musicId
                        : uploadResult.publicId())
                .width(uploadResult.width())
                .height(uploadResult.height())
                .mediaFormat(uploadResult.mediaFormat() == null
                        ? "mp3"
                        : uploadResult.mediaFormat())
                .resourceType(uploadResult.resourceType() == null
                        ? "video"
                        : uploadResult.resourceType())
                .bytes(uploadResult.bytes())
                .url(uploadResult.url())
                .secureUrl(uploadResult.secureUrl())
                .ownerId(musicId)
                .ownerType(OwnerType.MUSIC)
                .displayName(displayName)
                .version(uploadResult.version())
                .versionId(uploadResult.versionId())
                .createdAt(now)
                .updatedAt(now)
                .build();

        return r2dbcEntityTemplate.insert(Media.class).using(media)
                .doOnSuccess(saved -> log.info(
                        "|MediaService|saveImportedMusicMedia|saved|musicId={}|assetId={}|publicId={}",
                        musicId, saved.getAssetId(), saved.getPublicId()))
                .doOnError(error -> log.error(
                        "|MediaService|saveImportedMusicMedia|failed|musicId={}|error={}",
                        musicId, error.getMessage()))
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

        log.info("|MediaService|getProfileMedia|start|userId={}|ownerType={}|page={}|size={}",
                userId, ownerType, pageNumber, pageSize);
        return countMono.flatMap(total -> mediaFlux.collectList()
                        .doOnSuccess(items -> log.info("|MediaService|getProfileMedia|dbResult|userId={}|ownerType={}|count={}|total={}",
                                userId, ownerType, items.size(), total))
                        .map(items -> PageResponse.of(items, pageNumber, total, pageSize)))
                .doOnError(error -> log.error("|MediaService|getProfileMedia|failed|userId={}|ownerType={}|error={}",
                        userId, ownerType, error.getMessage()))
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

        log.info("|MediaService|getCurrentAvatar|start|userId={}", userId);
        return mediaRepository.findFirstByOwnerIdAndOwnerTypeOrderByCreatedAtDesc(userId, OwnerType.AVATAR)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.MEDIA_NOT_FOUND,
                        String.format("Current avatar not found for userId=%s", userId)
                )))
                .doOnSuccess(media -> log.info("|MediaService|getCurrentAvatar|success|userId={}|assetId={}",
                        userId, media.getAssetId()))
                .doOnError(error -> log.error("|MediaService|getCurrentAvatar|failed|userId={}|error={}",
                        userId, error.getMessage()));
    }

    public Mono<List<Media>> saveCloudinaryMediaList(List<String> publicIds, String ownerId, OwnerType ownerType) {
        int publicIdCount = publicIds == null ? 0 : publicIds.size();
        log.info("|MediaService|saveCloudinaryMediaList|start|ownerId={}|ownerType={}|publicIdCount={}",
                ownerId, ownerType, publicIdCount);
        return Flux.fromIterable(publicIds == null ? List.<String>of() : publicIds)
                .filter(publicId -> publicId != null && !publicId.isBlank())
                .concatMap(publicId -> saveCloudinaryMedia(publicId, ownerId, ownerType))
                .collectList()
                .doOnSuccess(saved -> log.info("|MediaService|saveCloudinaryMediaList|completed|ownerId={}|ownerType={}|savedCount={}",
                        ownerId, ownerType, saved.size()))
                .doOnError(error -> log.error("|MediaService|saveCloudinaryMediaList|failed|ownerId={}|ownerType={}|error={}",
                        ownerId, ownerType, error.getMessage()));
    }

}
