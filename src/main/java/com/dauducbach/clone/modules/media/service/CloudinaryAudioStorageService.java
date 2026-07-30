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

import java.util.Map;

@Service
@RequiredArgsConstructor
public class CloudinaryAudioStorageService {
    private static final Logger log = LoggerFactory.getLogger(CloudinaryAudioStorageService.class);
    private static final String MUSIC_FOLDER = "social_network_musics";

    private final Cloudinary cloudinary;

    public Mono<MediaAudioUploadResult> uploadMusic(byte[] bytes, String publicId) {
        if (bytes == null || bytes.length == 0) {
            return Mono.error(new IllegalArgumentException("Audio bytes are required"));
        }
        if (publicId == null || publicId.isBlank()) {
            return Mono.error(new IllegalArgumentException("Audio publicId is required"));
        }

        return Mono.fromCallable(() -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> result = cloudinary.uploader().upload(bytes, ObjectUtils.asMap(
                            "resource_type", "video",
                            "folder", MUSIC_FOLDER,
                            "public_id", publicId.trim()
                    ));
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
        return new MediaAudioUploadResult(
                stringValue(result.get("asset_id")),
                stringValue(result.get("public_id")),
                intValue(result.get("width")),
                intValue(result.get("height")),
                stringValue(result.get("format")),
                stringValue(result.get("resource_type")),
                intValue(result.get("bytes")),
                stringValue(result.get("url")),
                stringValue(result.get("secure_url")),
                stringValue(result.get("version")),
                stringValue(result.get("version_id"))
        );
    }

    private String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private int intValue(Object value) {
        return value instanceof Number number ? number.intValue() : 0;
    }
}
