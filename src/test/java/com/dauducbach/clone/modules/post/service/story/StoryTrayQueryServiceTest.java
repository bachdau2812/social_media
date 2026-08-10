package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.user.dto.response.UserDiscoveryResponse;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.post.entity.story.StoryView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.media.repositoty.music.MusicsRepository;
import com.dauducbach.clone.modules.post.repositoty.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repositoty.story.UserStoriesRepository;
import com.dauducbach.clone.modules.user.service.UserDiscoveryHydrator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryTrayQueryServiceTest {
    @Mock UserStoriesRepository userStoriesRepository;
    @Mock MusicsRepository musicsRepository;
    @Mock StoryViewRepository storyViewRepository;
    @Mock UserDiscoveryHydrator userDiscoveryHydrator;
    @Mock MediaCompatibilityFacade mediaFacade;

    @Test
    void excludesStoriesThatHaveExpiredEvenIfTheDatabaseReturnsThem() {
        UserStories expired = UserStories.builder()
                .id("expired-story")
                .userId("author-1")
                .mediaUrl("https://cdn.example.test/story.jpg")
                .mediaType("IMAGE")
                .status("APPROVED")
                .createdAt(Instant.now().minusSeconds(90_000))
                .expiredAt(Instant.now().minusSeconds(3_600))
                .build();
        when(userStoriesRepository.findHomeStoryTray(org.mockito.ArgumentMatchers.eq("viewer-1"), any(Instant.class), org.mockito.ArgumentMatchers.eq(20)))
                .thenReturn(Flux.just(expired));

        StoryTrayQueryService service = new StoryTrayQueryService(
                userStoriesRepository,
                storyViewRepository,
                userDiscoveryHydrator,
                mediaFacade,
                new StoryPlaybackHydrator(musicsRepository, new StoryMusicSegmentPolicy())
        );

        StepVerifier.create(service.getHomeStoryTray("viewer-1"))
                .expectNext(List.of())
                .verifyComplete();
    }

    @Test
    void derivesImageStoryDurationFromSelectedMusicSegment() {
        Instant now = Instant.now();
        UserStories story = UserStories.builder()
                .id("story-1")
                .userId("author-1")
                .mediaUrl("https://cdn.example.test/story.jpg")
                .mediaType("IMAGE")
                .musicId("music-1")
                .musicStart(12L)
                .musicEnd(57L)
                .status("APPROVED")
                .createdAt(now.minusSeconds(60))
                .expiredAt(now.plusSeconds(3_600))
                .build();
        when(userStoriesRepository.findHomeStoryTray(
                org.mockito.ArgumentMatchers.eq("viewer-1"),
                any(Instant.class),
                org.mockito.ArgumentMatchers.eq(20)))
                .thenReturn(Flux.just(story));
        when(storyViewRepository.findByViewerIdAndStoryIdIn(
                org.mockito.ArgumentMatchers.eq("viewer-1"), anyList()))
                .thenReturn(Flux.just(StoryView.builder()
                        .id("view-1")
                        .storyId("story-1")
                        .viewerId("viewer-1")
                        .reaction("LIKE")
                        .viewedAt(now)
                        .build()));
        when(musicsRepository.findAllById(any(Iterable.class)))
                .thenReturn(Flux.just(Musics.builder()
                        .id("music-1")
                        .songUrl("/music.mp3")
                        .fetched(true)
                        .build()));
        when(userDiscoveryHydrator.hydrate("author-1", "author-1"))
                .thenReturn(Mono.just(new UserDiscoveryResponse(
                        "author-1",
                        "author",
                        "Author",
                        "/avatar.jpg",
                        false,
                        false,
                        false,
                        "SELF")));
        when(mediaFacade.transformDeliveryUrl(story.getMediaUrl(), MediaDisplayType.STORY))
                .thenReturn(story.getMediaUrl());

        StoryTrayQueryService service = new StoryTrayQueryService(
                userStoriesRepository,
                storyViewRepository,
                userDiscoveryHydrator,
                mediaFacade,
                new StoryPlaybackHydrator(musicsRepository, new StoryMusicSegmentPolicy())
        );

        StepVerifier.create(service.getHomeStoryTray("viewer-1"))
                .assertNext(items -> org.assertj.core.api.Assertions.assertThat(items)
                        .singleElement()
                        .satisfies(item -> {
                            org.assertj.core.api.Assertions.assertThat(item.durationSeconds()).isEqualTo(45L);
                            org.assertj.core.api.Assertions.assertThat(item.viewerSeen()).isTrue();
                            org.assertj.core.api.Assertions.assertThat(item.viewerReaction()).isEqualTo("LIKE");
                        }))
                .verifyComplete();
    }
}
