package com.dauducbach.clone.modules.notification.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class NotificationMetadataCodec {
    private static final Logger log = LoggerFactory.getLogger(NotificationMetadataCodec.class);
    private static final TypeReference<Map<String, String>> METADATA_TYPE = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;

    public NotificationMetadataCodec(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String encode(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(new LinkedHashMap<>(metadata));
        } catch (JsonProcessingException error) {
            log.warn("|NotificationMetadataCodec|encode|failed|error={}", error.getMessage());
            return null;
        }
    }

    public Map<String, String> decode(String metadata) {
        if (metadata == null || metadata.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(metadata, METADATA_TYPE);
        } catch (JsonProcessingException error) {
            log.warn("|NotificationMetadataCodec|decode|failed|error={}", error.getMessage());
            return Map.of();
        }
    }
}
