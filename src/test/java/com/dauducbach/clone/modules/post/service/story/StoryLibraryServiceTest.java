package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.service.StoryReplyMessaging;
import com.dauducbach.clone.modules.post.dto.story.request.StoryReplyRequest;
import com.dauducbach.clone.modules.post.dto.story.response.StoryArchiveResponse;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.entity.story.StoryHighlight;
import com.dauducbach.clone.modules.post.entity.story.StoryHighlightItem;
import com.dauducbach.clone.modules.post.repositoty.story.StoryHighlightItemRepository;
import com.dauducbach.clone.modules.post.repositoty.story.StoryHighlightRepository;
import com.dauducbach.clone.modules.post.repositoty.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repositoty.story.UserStoriesRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
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
                mock(R2dbcEntityTemplate.class), mock(DatabaseClient.class),
                mock(StoryReplyMessaging.class), playbackHydrator);

        StepVerifier.create(service.listHighlights("owner-1"))
                .assertNext(response -> {
                    assertThat(response.stories()).singleElement().isEqualTo(playback);
                    assertThat(response.stories().getFirst().musicUrl()).isEqualTo("/music.mp3");
                    assertThat(response.stories().getFirst().durationSeconds()).isEqualTo(30L);
                })
                .verifyComplete();
    }

    @Test
    void recordsAViewWithOneAtomicUpsert() {
        UserStoriesRepository stories = mock(UserStoriesRepository.class);
        StoryViewRepository views = mock(StoryViewRepository.class);
        StoryReplyMessaging messaging = mock(StoryReplyMessaging.class);
        StoryLibraryService service = service(stories, views, messaging);
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

    @Test
    void rejectsOwnRemovedAndExpiredStoriesBeforeCallingChat() {
        UserStoriesRepository stories = mock(UserStoriesRepository.class);
        StoryReplyMessaging messaging = mock(StoryReplyMessaging.class);
        StoryLibraryService service = service(stories, messaging);
        Instant now = Instant.now();
        UserStories own = story("own", "actor-1", "APPROVED", now.plusSeconds(3600));
        UserStories removed = story("removed", "owner-1", "REMOVED", now.plusSeconds(3600));
        UserStories expired = story("expired", "owner-1", "APPROVED", now.minusSeconds(1));
        when(stories.findById("own")).thenReturn(Mono.just(own));
        when(stories.findById("removed")).thenReturn(Mono.just(removed));
        when(stories.findById("expired")).thenReturn(Mono.just(expired));
        StoryReplyRequest request = new StoryReplyRequest(
                "hello", UUID.randomUUID().toString(), 0L);

        assertStoryError(service.reply("own", "actor-1", request));
        assertStoryError(service.reply("removed", "actor-1", request));
        assertStoryError(service.reply("expired", "actor-1", request));

        verify(messaging, never()).send(org.mockito.ArgumentMatchers.any());
    }

    private StoryLibraryService service(UserStoriesRepository stories, StoryReplyMessaging messaging) {
        return service(stories, mock(StoryViewRepository.class), messaging);
    }

    private StoryLibraryService service(
            UserStoriesRepository stories,
            StoryViewRepository views,
            StoryReplyMessaging messaging
    ) {
        return new StoryLibraryService(
                stories,
                views,
                mock(StoryHighlightRepository.class),
                mock(StoryHighlightItemRepository.class),
                mock(R2dbcEntityTemplate.class),
                mock(DatabaseClient.class),
                messaging,
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

    private void assertStoryError(Mono<?> result) {
        StepVerifier.create(result)
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(AppException.class);
                    assertThat(((AppException) error).getErrorCode())
                            .isIn(ErrorCode.STORY_SAVE_FAILED, ErrorCode.STORY_NOT_FOUND);
                })
                .verify();
    }
}
