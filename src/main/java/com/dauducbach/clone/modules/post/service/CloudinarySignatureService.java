package com.dauducbach.clone.modules.post.service;

import com.cloudinary.Cloudinary;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.dto.response.MediaSignatureResponse;
import com.cloudinary.utils.ObjectUtils;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class CloudinarySignatureService {
    private static final Logger log = LoggerFactory.getLogger(CloudinarySignatureService.class);
    private static final String FOLDER = "social_network_posts";
    private static final String UPLOAD_PRESET = "ml_default";

    final Cloudinary cloudinary;

    @Value("${cloudinary.api-key}")
    String apiKey;

    public MediaSignatureResponse generateSignature() {
        long timestamp = Instant.now().getEpochSecond();
        log.info("|CloudinarySignatureService|generateSignature|folder={}|timestamp={}", FOLDER, timestamp);
        Map<String, Object> params = ObjectUtils.asMap(
                "timestamp", timestamp,
                "folder", FOLDER,
                "upload_preset", UPLOAD_PRESET
        );

        try {
            String signature = cloudinary.apiSignRequest(params, cloudinary.config.apiSecret);
            log.info("|CloudinarySignatureService|generateSignature|success|folder={}|timestamp={}", FOLDER, timestamp);
            return MediaSignatureResponse.builder()
                    .signature(signature)
                    .timestamp(timestamp)
                    .apiKey(apiKey)
                    .folder(FOLDER)
                    .uploadPreset(UPLOAD_PRESET)
                    .build();
        } catch (Exception ex) {
            log.error("|CloudinarySignatureService|generateSignature|failed|folder={}|timestamp={}|error={}",
                    FOLDER, timestamp, ex.getMessage());
            throw new AppException(ErrorCode.MEDIA_SIGNATURE_FAILED, "Generate upload signature failed", ex);
        }
    }
}

