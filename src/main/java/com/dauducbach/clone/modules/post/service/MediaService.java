package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.constant.OwnerType;
import com.dauducbach.clone.modules.post.entity.Media;
import com.dauducbach.clone.modules.post.repositoty.MediaRepository;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
@Slf4j
public class MediaService {
    MediaRepository mediaRepository;

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
}
