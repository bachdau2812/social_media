package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.user.entity.UserStories;
import com.dauducbach.clone.modules.user.repositoty.MusicsRepository;
import com.dauducbach.clone.modules.user.repositoty.StoryViewRepository;
import com.dauducbach.clone.modules.user.repositoty.UserStoriesRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
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
                musicsRepository,
                storyViewRepository,
                userDiscoveryHydrator,
                mediaFacade
        );

        StepVerifier.create(service.getHomeStoryTray("viewer-1"))
                .expectNext(List.of())
                .verifyComplete();
    }
}
