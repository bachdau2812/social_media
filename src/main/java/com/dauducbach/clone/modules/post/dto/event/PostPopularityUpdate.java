package com.dauducbach.clone.modules.post.dto.event;

import java.time.Instant;

public record PostPopularityUpdate(int schemaVersion, String eventId, String postId,
                                   Instant popularSince, Instant expiresAt, long qualificationScore,
                                   String algorithmVersion) {}
