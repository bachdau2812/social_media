package com.dauducbach.clone.modules.media.infrastructure.cloudinary;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.Collection;

@Service
@RequiredArgsConstructor
public class MediaAssetCleanupService {
    private static final Logger log = LoggerFactory.getLogger(MediaAssetCleanupService.class);

    private final Cloudinary cloudinary;

    public Mono<Void> deleteAll(Collection<String> publicIds) {
        if (publicIds == null || publicIds.isEmpty()) {
            return Mono.empty();
        }
        return Flux.fromIterable(publicIds)
                .filter(publicId -> publicId != null && !publicId.isBlank())
                .distinct()
                .flatMap(this::delete)
                .then();
    }

    public Mono<Void> delete(String publicId) {
        if (publicId == null || publicId.isBlank()) {
            return Mono.empty();
        }
        return Mono.fromCallable(() -> {
                    try {
                        var result = cloudinary.uploader().destroy(publicId, ObjectUtils.emptyMap());
                        log.info("|MediaAssetCleanupService|delete|publicId={}|resourceType=image|result={}",
                                publicId, result == null ? null : result.get("result"));
                    } catch (Exception imageError) {
                        log.warn("|MediaAssetCleanupService|delete|image skipped|publicId={}|error={}",
                                publicId, imageError.getMessage());
                    }
                    try {
                        var result = cloudinary.uploader().destroy(
                                publicId,
                                ObjectUtils.asMap("resource_type", "video"));
                        log.info("|MediaAssetCleanupService|delete|publicId={}|resourceType=video|result={}",
                                publicId, result == null ? null : result.get("result"));
                        return result;
                    } catch (Exception videoError) {
                        log.warn("|MediaAssetCleanupService|delete|video skipped|publicId={}|error={}",
                                publicId, videoError.getMessage());
                        return ObjectUtils.emptyMap();
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .onErrorResume(error -> {
                    log.error("|MediaAssetCleanupService|delete|publicId={}|error={}",
                            publicId, error.getMessage());
                    return Mono.empty();
                })
                .then();
    }
}
