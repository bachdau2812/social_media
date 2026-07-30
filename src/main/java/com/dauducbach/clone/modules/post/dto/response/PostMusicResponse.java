package com.dauducbach.clone.modules.post.dto.response;

public record PostMusicResponse(
        String id,
        String displayName,
        String artist,
        String artworkUrl,
        String playbackUrl,
        Long segmentStart,
        Long segmentEnd,
        Long duration
) {
}
