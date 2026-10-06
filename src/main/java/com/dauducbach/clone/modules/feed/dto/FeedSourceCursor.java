package com.dauducbach.clone.modules.feed.dto;

import java.time.Instant;

/** Retains DB timestamp precision so equal-millisecond posts cannot be skipped. */
public record FeedSourceCursor(long timeMs, String postId, Instant preciseTime) {
    public FeedSourceCursor(long timeMs, String postId) { this(timeMs, postId, null); }
    public Instant orderTime() { return preciseTime == null ? Instant.ofEpochMilli(timeMs) : preciseTime; }
}
