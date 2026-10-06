package com.dauducbach.clone.modules.streams.post.state;

public record ImpressionState(long firstOccurredAt, long lastOccurredAt, long expiresAt,
                              boolean clicked, int maxViewTime, int earnedScore) {}
