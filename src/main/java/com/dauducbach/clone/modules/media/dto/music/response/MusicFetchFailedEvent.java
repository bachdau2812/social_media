package com.dauducbach.clone.modules.media.dto.music.response;

public record MusicFetchFailedEvent(
        String trackId,
        String message
) {
}
