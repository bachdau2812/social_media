package com.dauducbach.clone.modules.media.service;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class CloudinaryAudioStorageService {
    private static final Logger log = LoggerFactory.getLogger(CloudinaryAudioStorageService.class);
    private static final String MUSIC_FOLDER = "social_network_musics";

    private final Cloudinary cloudinary;

    public Mono<MediaAudioUploadResult> uploadMusic(Path file, String publicId) {
        if (file == null || !Files.isRegularFile(file)) {
            return Mono.error(new IllegalArgumentException("Audio file is required"));
        }
        if (publicId == null || publicId.isBlank()) {
            return Mono.error(new IllegalArgumentException("Audio publicId is required"));
        }

        return Mono.fromCallable(() -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> result = cloudinary.uploader().upload(
                            file.toFile(),
                            ObjectUtils.asMap(
                                    "resource_type", "video",
                                    "folder", MUSIC_FOLDER,
                                    "public_id", publicId.trim(),
                                    "overwrite", true));
                    return toResult(result);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .doOnSuccess(result -> log.info(
                        "|CloudinaryAudioStorageService|uploadMusic|success|publicId={}",
                        result.publicId()))
                .doOnError(error -> log.error(
                        "|CloudinaryAudioStorageService|uploadMusic|failed|publicId={}|error={}",
                        publicId, error.getMessage()));
    }

    private MediaAudioUploadResult toResult(Map<String, Object> result) {
        String publicId = requiredValue(result, "public_id");
        String version = requiredValue(result, "version");
        String resourceType = firstNonBlank(stringValue(result.get("resource_type")), "video");
        String deliveryUrl = mp3DeliveryUrl(publicId, version, resourceType, false);
        String secureDeliveryUrl = mp3DeliveryUrl(publicId, version, resourceType, true);

        return new MediaAudioUploadResult(
                stringValue(result.get("asset_id")),
                publicId,
                intValue(result.get("width")),
                intValue(result.get("height")),
                stringValue(result.get("format")),
                resourceType,
                intValue(result.get("bytes")),
                deliveryUrl,
                secureDeliveryUrl,
                version,
                stringValue(result.get("version_id"))
        );
    }

    private String mp3DeliveryUrl(
            String publicId,
            String version,
            String resourceType,
            boolean secure) {
        return cloudinary.url()
                .resourceType(resourceType)
                .type("upload")
                .version(version)
                .format("mp3")
                .secure(secure)
                .generate(publicId);
    }

    private String requiredValue(Map<String, Object> result, String key) {
        String value = stringValue(result.get(key));
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Cloudinary upload result is missing " + key);
        }
        return value;
    }

    private String firstNonBlank(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }

    private String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private int intValue(Object value) {
        return value instanceof Number number ? number.intValue() : 0;
    }
}
