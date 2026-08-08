package com.dauducbach.clone.modules.media.service.music;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

public record SpotifyMusicMetadata(
        String title,
        String artist,
        String album,
        String albumArtist,
        String composer,
        String genre,
        String lyrics
) {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public String descriptionsJson() {
        Map<String, String> descriptions = new LinkedHashMap<>();
        putIfPresent(descriptions, "COMPOSER", composer);
        putIfPresent(descriptions, "ALBUM_ARTIST", albumArtist);
        putIfPresent(descriptions, "LYRICS", lyrics);
        try {
            return OBJECT_MAPPER.writeValueAsString(descriptions);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Could not serialize music descriptions", error);
        }
    }

    private void putIfPresent(Map<String, String> values, String key, String value) {
        if (value != null && !value.isBlank()) {
            values.put(key, value);
        }
    }
}
