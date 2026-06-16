package com.dauducbach.clone.modules.post.service;

import com.cloudinary.Cloudinary;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.dto.response.MediaSignatureResponse;
import com.cloudinary.utils.ObjectUtils;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class CloudinarySignatureService {
    private static final String FOLDER = "social_network_posts";
    private static final String UPLOAD_PRESET = "ml_default";

    final Cloudinary cloudinary;

    @Value("${cloudinary.api-key}")
    String apiKey;

    public MediaSignatureResponse generateSignature() {
        long timestamp = Instant.now().getEpochSecond();
        Map<String, Object> params = ObjectUtils.asMap(
                "timestamp", timestamp,
                "folder", FOLDER,
                "upload_preset", UPLOAD_PRESET
        );

        try {
            String signature = cloudinary.apiSignRequest(params, cloudinary.config.apiSecret);
            return MediaSignatureResponse.builder()
                    .signature(signature)
                    .timestamp(timestamp)
                    .apiKey(apiKey)
                    .folder(FOLDER)
                    .uploadPreset(UPLOAD_PRESET)
                    .build();
        } catch (Exception ex) {
            throw new AppException(ErrorCode.MEDIA_SIGNATURE_FAILED, "Generate upload signature failed", ex);
        }
    }
}

