package com.dauducbach.clone.modules.media.dto.music.internal;

import com.dauducbach.clone.modules.media.service.music.SpotifyMusicMetadata;

public record MusicArtifactMetadata(
        String title,
        String artist,
        String album,
        String albumArtist,
        String composer,
        String genre,
        String lyrics) {
    public SpotifyMusicMetadata toSpotifyMetadata() {
        return new SpotifyMusicMetadata(
                title, artist, album, albumArtist, composer, genre, lyrics);
    }
}
