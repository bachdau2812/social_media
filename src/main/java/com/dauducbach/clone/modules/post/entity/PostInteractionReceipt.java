package com.dauducbach.clone.modules.post.entity;

import java.time.Instant;

public record PostInteractionReceipt(String actorId, String eventId, String postId, String impressionId,
                                     String payloadHash, int computedScore, Instant acceptedAt) {}
