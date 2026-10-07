package com.dauducbach.clone.modules.frontend.dto;

import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMediaResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import com.dauducbach.clone.modules.post.dto.story.response.StoryTrayResponse;
import com.dauducbach.clone.modules.post.publicapi.PostPresentationSnapshot;
import com.dauducbach.clone.modules.post.publicapi.StoryTrayQuery;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScreenSnapshotContractTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void publicPostPresentationSnapshotPreservesNestedPostJson() {
        PostItemResponse response = new PostItemResponse(
                "item", 0, "caption",
                new PostMediaResponse("asset", "public", "jpg", "image", "url", "secure", "photo", 640, 480),
                new PostMusicResponse("music", "Track", "Artist", "art", "play", 1L, 8L, 9L));
        PostPresentationSnapshot.Item snapshot = new PostPresentationSnapshot.Item(
                "item", 0, "caption",
                new PostPresentationSnapshot.Media("asset", "public", "jpg", "image", "url", "secure", "photo", 640, 480),
                new PostPresentationSnapshot.Music("music", "Track", "Artist", "art", "play", 1L, 8L, 9L));

        assertEquals(asMap(response), asMap(snapshot));
    }

    @Test
    void publicStoryTraySnapshotPreservesExistingJson() {
        StoryTrayResponse response = new StoryTrayResponse(
                "story", "user", "alice", "Alice", "avatar", "media", "VIDEO", "music", "track",
                "Track", 1L, 8L, 9L, "APPROVED", null, null, "publication", 0, 1, true, "LIKE");
        StoryTrayQuery.StoryTraySnapshot snapshot = new StoryTrayQuery.StoryTraySnapshot(
                "story", "user", "alice", "Alice", "avatar", "media", "VIDEO", "music", "track",
                "Track", 1L, 8L, 9L, "APPROVED", null, null, "publication", 0, 1, true, "LIKE");

        assertEquals(asMap(response), asMap(snapshot));
    }

    private Map<?, ?> asMap(Object value) {
        return objectMapper.convertValue(value, Map.class);
    }
}
