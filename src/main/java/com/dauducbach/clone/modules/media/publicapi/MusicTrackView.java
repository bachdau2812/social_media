package com.dauducbach.clone.modules.media.publicapi;

import java.math.BigDecimal;

/** Stable read model for music consumers outside the media module. */
public record MusicTrackView(
        String id,
        String slugName,
        String displayName,
        String descriptions,
        String displayImages,
        String singleName,
        String songUrl,
        Long duration,
        String category,
        Short releaseYear,
        String albumName,
        Boolean fetched,
        BigDecimal popularity) {
}
