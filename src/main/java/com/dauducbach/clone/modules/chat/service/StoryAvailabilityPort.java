package com.dauducbach.clone.modules.chat.service;

import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;

public interface StoryAvailabilityPort {

    Mono<Map<StoryReference, StoryAvailability>> resolve(
            Collection<StoryReference> references,
            Instant now);

    record StoryReference(String storyId, long previewAtMs) {
    }

    record StoryAvailability(
            String storyId,
            boolean available,
            String mediaType,
            long previewAtMs,
            Instant expiresAt,
            String previewUrl) {
    }
}
