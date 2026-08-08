package com.dauducbach.clone.modules.media.dto.music.response;

public record MusicFetchAcceptedResponse(
        String trackId,
        Status status
) {
    public enum Status {
        STARTED,
        PROCESSING,
        ALREADY_FETCHED
    }
}
