package com.dauducbach.clone.modules.post.stories.infrastructure.kafka;

import com.dauducbach.clone.modules.post.stories.publishing.StoryMediaScanRequest;
import com.dauducbach.clone.modules.post.stories.publishing.StoryMediaScanService;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryMediaScanListenerTest {
    @Mock
    StoryMediaScanService storyMediaScanService;

    @Test
    void mapsKafkaPayloadToStoryScanCommand() {
        when(storyMediaScanService.scan(any())).thenReturn(Mono.empty());
        StoryMediaScanListener listener = new StoryMediaScanListener(storyMediaScanService);

        listener.handle(payload("story-1", "owner-1", "https://cdn.example/story.jpg").toString()).join();

        verify(storyMediaScanService).scan(new StoryMediaScanRequest(
                "story-1", "owner-1", "https://cdn.example/story.jpg", "stories/story_1", "IMAGE"));
    }

    @Test
    void ignoresPayloadWithoutRequiredIdentityOrMedia() {
        StoryMediaScanListener listener = new StoryMediaScanListener(storyMediaScanService);

        listener.handle(payload("", "owner-1", "https://cdn.example/story.jpg").toString()).join();

        verify(storyMediaScanService, never()).scan(any());
    }

    private JsonObject payload(String storyId, String userId, String mediaUrl) {
        JsonObject json = new JsonObject();
        json.addProperty("storyId", storyId);
        json.addProperty("userId", userId);
        json.addProperty("mediaUrl", mediaUrl);
        json.addProperty("publicId", "stories/story_1");
        json.addProperty("mediaType", "IMAGE");
        return json;
    }
}
