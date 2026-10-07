package com.dauducbach.clone.modules.post.popularity.infrastructure.streams.state;

public record ImpressionState(long firstOccurredAt, long lastOccurredAt, long expiresAt,
                              boolean clicked, int maxViewTime, int earnedScore) {}
