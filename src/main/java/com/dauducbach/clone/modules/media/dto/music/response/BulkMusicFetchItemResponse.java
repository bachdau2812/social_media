package com.dauducbach.clone.modules.media.dto.music.response;

public record BulkMusicFetchItemResponse(
        String trackId,
        Status status,
        String message
) {
    public enum Status {
        STARTED,
        PROCESSING,
        ALREADY_FETCHED,
        FAILED
    }
}
