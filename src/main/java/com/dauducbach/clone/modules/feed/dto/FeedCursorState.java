package com.dauducbach.clone.modules.feed.dto;

public record FeedCursorState(
        String viewer, String media, String version, long startedAt, long expiresAt,
        FeedSourceCursor friends, FeedSourceCursor popular,
        boolean friendsExhausted, boolean popularExhausted
) {}
