package com.dauducbach.clone.modules.post.stories.library;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.dto.story.response.StoryArchiveResponse;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.entity.story.StoryHighlight;
import com.dauducbach.clone.modules.post.entity.story.StoryHighlightItem;
import com.dauducbach.clone.modules.post.stories.playback.StoryPlaybackHydrator;
import com.dauducbach.clone.modules.post.repository.story.StoryHighlightItemRepository;
import com.dauducbach.clone.modules.post.repository.story.StoryHighlightRepository;
import com.dauducbach.clone.modules.post.repository.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repository.story.StoryViewQueryRepository;
import com.dauducbach.clone.modules.post.repository.story.projection.StoryViewerRow;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.UUID;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StoryLibraryServiceTest {

    @Test
    void highlightsExposeHydratedStoryPlayback() {
        UserStoriesRepository stories = mock(UserStoriesRepository.class);
        StoryViewRepository views = mock(StoryViewRepository.class);
        StoryHighlightRepository highlights = mock(StoryHighlightRepository.class);
        StoryHighlightItemRepository items = mock(StoryHighlightItemRepository.class);
        StoryViewQueryRepository viewerQuery = mock(StoryViewQueryRepository.class);
        UserIdentityQuery identities = mock(UserIdentityQuery.class);
        StoryPlaybackHydrator playbackHydrator = mock(StoryPlaybackHydrator.class);
        StoryHighlight highlight = StoryHighlight.builder()
                .id("highlight-1")
                .ownerId("owner-1")
                .title("Featured")
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        UserStories story = story("story-1", "owner-1", "APPROVED", Instant.now().plusSeconds(3600));
        story.setMusicId("music-1");
        story.setMusicStart(10L);
        story.setMusicEnd(40L);
        StoryArchiveResponse playback = new StoryArchiveResponse(
                "story-1", "owner-1", story.getMediaUrl(), "IMAGE",
                "music-1", "/music.mp3", "Story song", 10L, 40L, 30L,
                null, null, null, "APPROVED", story.getCreatedAt(), story.getExpiredAt(), null);

        when(highlights.findByOwnerIdOrderByUpdatedAtDesc("owner-1")).thenReturn(Flux.just(highlight));
        when(items.findByHighlightIdOrderByOrderNumberAsc("highlight-1"))
                .thenReturn(Flux.just(StoryHighlightItem.builder()
                        .id("item-1").highlightId("highlight-1").storyId("story-1").orderNumber(1).build()));
        when(stories.findById("story-1")).thenReturn(Mono.just(story));
        when(playbackHydrator.hydrateAll(any(), any())).thenReturn(Mono.just(List.of(playback)));

        StoryLibraryService service = new StoryLibraryService(
                stories, views, highlights, items,
                mock(R2dbcEntityTemplate.class), viewerQuery, identities,
                playbackHydrator);

        StepVerifier.create(service.listHighlights("owner-1"))
                .assertNext(response -> {
                    assertThat(response.stories()).singleElement().isEqualTo(playback);
                    assertThat(response.stories().getFirst().musicUrl()).isEqualTo("/music.mp3");
                    assertThat(response.stories().getFirst().durationSeconds()).isEqualTo(30L);
                })
                .verifyComplete();
    }

    @Test
    void viewersUsePostOwnedRowsAndOneBatchedUserIdentityQuery() {
        UserStoriesRepository stories = mock(UserStoriesRepository.class);
        StoryViewRepository views = mock(StoryViewRepository.class);
        StoryViewQueryRepository viewerQuery = mock(StoryViewQueryRepository.class);
        UserIdentityQuery identities = mock(UserIdentityQuery.class);
        Instant firstViewedAt = Instant.parse("2026-10-06T08:00:00Z");
        Instant secondViewedAt = Instant.parse("2026-10-06T07:00:00Z");
        when(stories.findById("story-1"))
                .thenReturn(Mono.just(story("story-1", "owner-1", "APPROVED", Instant.now().plusSeconds(3600))));
        when(viewerQuery.findViewerPage("story-1", 50, 0)).thenReturn(Flux.just(
                new StoryViewerRow("viewer-1", "LIKE", firstViewedAt),
                new StoryViewerRow("viewer-2", null, secondViewedAt)));
        when(views.countByStoryId("story-1")).thenReturn(Mono.just(2L));
        when(identities.findIdentities(any()))
                .thenReturn(Flux.just(new UserIdentity("viewer-1", "first", "First Viewer", "avatar-1")));
        StoryLibraryService service = new StoryLibraryService(
                stories, views, mock(StoryHighlightRepository.class), mock(StoryHighlightItemRepository.class),
                mock(R2dbcEntityTemplate.class), viewerQuery, identities, mock(StoryPlaybackHydrator.class));

        StepVerifier.create(service.viewers("story-1", "owner-1", 0, 50))
                .assertNext(page -> {
                    assertThat(page.totalElements()).isEqualTo(2);
                    assertThat(page.content()).containsExactly(
                            new com.dauducbach.clone.modules.post.dto.story.response.StoryViewerResponse(
                                    "viewer-1", "first", "First Viewer", "avatar-1", "LIKE", firstViewedAt),
                            new com.dauducbach.clone.modules.post.dto.story.response.StoryViewerResponse(
                                    "viewer-2", null, null, null, null, secondViewedAt));
                })
                .verifyComplete();

        verify(viewerQuery).findViewerPage("story-1", 50, 0);
        verify(identities).findIdentities(List.of("viewer-1", "viewer-2"));
    }

    @Test
    void recordsAViewWithOneAtomicUpsert() {
        UserStoriesRepository stories = mock(UserStoriesRepository.class);
        StoryViewRepository views = mock(StoryViewRepository.class);
        StoryLibraryService service = service(stories, views);
        when(stories.findById("story-1"))
                .thenReturn(Mono.just(story(
                        "story-1", "owner-1", "APPROVED", Instant.now().plusSeconds(3600))));
        when(views.upsertView(
                anyString(), eq("story-1"), eq("viewer-1"), isNull(), any(Instant.class)))
                .thenReturn(Mono.just(1));

        StepVerifier.create(service.recordView("story-1", "viewer-1", null))
                .verifyComplete();

        verify(views).upsertView(
                anyString(), eq("story-1"), eq("viewer-1"), isNull(), any(Instant.class));
        verify(views, never()).findByStoryIdAndViewerId(anyString(), anyString());
    }

    private StoryLibraryService service(UserStoriesRepository stories) {
        return service(stories, mock(StoryViewRepository.class));
    }

    private StoryLibraryService service(
            UserStoriesRepository stories,
            StoryViewRepository views
    ) {
        return new StoryLibraryService(
                stories,
                views,
                mock(StoryHighlightRepository.class),
                mock(StoryHighlightItemRepository.class),
                mock(R2dbcEntityTemplate.class),
                mock(StoryViewQueryRepository.class),
                mock(UserIdentityQuery.class),
                mock(StoryPlaybackHydrator.class));
    }

    private UserStories story(String id, String ownerId, String status, Instant expiresAt) {
        return UserStories.builder()
                .id(id)
                .userId(ownerId)
                .mediaType("IMAGE")
                .mediaUrl("https://host/story.jpg")
                .status(status)
                .createdAt(Instant.now().minusSeconds(60))
                .expiredAt(expiresAt)
                .build();
    }

}
