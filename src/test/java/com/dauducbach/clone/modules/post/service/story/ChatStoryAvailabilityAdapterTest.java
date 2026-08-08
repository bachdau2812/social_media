package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.chat.service.StoryAvailabilityPort;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repositoty.story.UserStoriesRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChatStoryAvailabilityAdapterTest {

    @Test
    void resolvesAvailableVideoStillAndUnavailableStoriesInOneBatch() {
        UserStoriesRepository repository = mock(UserStoriesRepository.class);
        MediaCompatibilityFacade media = mock(MediaCompatibilityFacade.class);
        ChatStoryAvailabilityAdapter adapter = new ChatStoryAvailabilityAdapter(repository, media);
        Instant now = Instant.parse("2026-07-31T00:00:00Z");
        UserStories activeVideo = UserStories.builder()
                .id("video-1")
                .userId("owner-1")
                .mediaType("VIDEO")
                .mediaUrl("https://res.cloudinary.com/demo/video/upload/v1/story.mp4")
                .status("APPROVED")
                .createdAt(now.minusSeconds(60))
                .expiredAt(now.plusSeconds(3600))
                .build();
        UserStories removed = UserStories.builder()
                .id("removed-1")
                .userId("owner-2")
                .mediaType("IMAGE")
                .mediaUrl("https://host/removed.jpg")
                .status("REMOVED")
                .createdAt(now.minusSeconds(60))
                .expiredAt(now.plusSeconds(3600))
                .build();
        StoryAvailabilityPort.StoryReference video =
                new StoryAvailabilityPort.StoryReference("video-1", 12400L);
        StoryAvailabilityPort.StoryReference removedReference =
                new StoryAvailabilityPort.StoryReference("removed-1", 0L);
        StoryAvailabilityPort.StoryReference missing =
                new StoryAvailabilityPort.StoryReference("missing-1", 0L);
        when(repository.findAllById(any(Iterable.class))).thenReturn(Flux.just(activeVideo, removed));
        when(media.storyVideoStill(activeVideo.getMediaUrl(), 12400L)).thenReturn("https://host/still.jpg");

        StepVerifier.create(adapter.resolve(List.of(video, removedReference, missing), now))
                .assertNext(result -> {
                    assertThat(result.get(video).available()).isTrue();
                    assertThat(result.get(video).previewUrl()).isEqualTo("https://host/still.jpg");
                    assertThat(result.get(removedReference).available()).isFalse();
                    assertThat(result.get(removedReference).previewUrl()).isNull();
                    assertThat(result.get(missing).available()).isFalse();
                })
                .verifyComplete();

        verify(repository, times(1)).findAllById(any(Iterable.class));
        verify(media, times(1)).storyVideoStill(activeVideo.getMediaUrl(), 12400L);
    }
}
