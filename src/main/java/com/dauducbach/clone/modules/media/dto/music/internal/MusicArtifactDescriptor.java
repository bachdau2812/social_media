package com.dauducbach.clone.modules.media.dto.music.internal;

import java.time.Instant;

public record MusicArtifactDescriptor(
        String artifactId,
        String trackId,
        String filename,
        String contentType,
        long sizeBytes,
        String sha256,
        Instant expiresAt,
        MusicArtifactMetadata metadata) {
}
