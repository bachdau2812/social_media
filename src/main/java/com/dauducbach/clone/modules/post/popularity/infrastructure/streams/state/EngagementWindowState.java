package com.dauducbach.clone.modules.post.popularity.infrastructure.streams.state;

public record EngagementWindowState(long score, long maxOccurredAt, long triggerOccurredAt) {}
