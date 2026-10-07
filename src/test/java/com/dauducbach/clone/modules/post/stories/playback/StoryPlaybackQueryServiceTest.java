package com.dauducbach.clone.modules.post.stories.playback;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.post.dto.story.response.StoryArchiveResponse;
import com.dauducbach.clone.modules.post.entity.story.StoryView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repository.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveSelectOperation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StoryPlaybackQueryServiceTest {
    @Test
    void hydratesViewerSeenAndPlaybackFieldsFromQueryDependencies() {
        UserStoriesRepository stories = mock(UserStoriesRepository.class);
        StoryViewRepository views = mock(StoryViewRepository.class);
        R2dbcEntityTemplate entityTemplate = mock(R2dbcEntityTemplate.class);
        MediaAssets media = mock(MediaAssets.class);
        StoryPlaybackHydrator hydrator = mock(StoryPlaybackHydrator.class);
        StoryPlaybackQueryService service = new StoryPlaybackQueryService(stories, views, entityTemplate, media, hydrator);
        UserStories story = UserStories.builder()
                .id("story-1").userId("owner-1").mediaUrl("https://cdn.example/story.jpg")
                .mediaType("IMAGE").status("APPROVED")
                .createdAt(Instant.parse("2026-09-30T00:00:00Z"))
                .expiredAt(Instant.now().plusSeconds(3600)).build();
        ReactiveSelectOperation.ReactiveSelect<StoryView> select = mock(ReactiveSelectOperation.ReactiveSelect.class);
        ReactiveSelectOperation.TerminatingSelect<StoryView> terminal = mock(ReactiveSelectOperation.TerminatingSelect.class);
        when(stories.countActiveApprovedByUserId(eq("owner-1"), any(Instant.class))).thenReturn(Mono.just(1L));
        when(stories.findActiveApprovedByUserId(eq("owner-1"), any(Instant.class), eq(20), eq(0L)))
                .thenReturn(Flux.just(story));
        when(entityTemplate.select(StoryView.class)).thenReturn(select);
        when(select.matching(any())).thenReturn(terminal);
        when(terminal.all()).thenReturn(Flux.just(StoryView.builder().storyId("story-1").viewerId("viewer-1").build()));
        when(media.transformDeliveryUrl("https://cdn.example/story.jpg", MediaDisplayType.STORY))
                .thenReturn("https://cdn.example/story-playback.jpg");
        when(hydrator.hydrateAll(any(), any())).thenReturn(Mono.just(List.of(new StoryArchiveResponse(
                "story-1", "owner-1", "https://cdn.example/story-playback.jpg", "IMAGE",
                "music-1", "https://cdn.example/music.mp3", "Story song", 10L, 40L, 30L,
                null, null, null, "APPROVED", story.getCreatedAt(), story.getExpiredAt(), true))));

        StepVerifier.create(service.getStories("owner-1", "viewer-1", 0, 20, MediaDisplayType.STORY))
                .assertNext(page -> {
                    assertThat(page.content()).singleElement().satisfies(item -> {
                        assertThat(item.id()).isEqualTo("story-1");
                        assertThat(item.viewerSeen()).isTrue();
                        assertThat(item.mediaUrl()).isEqualTo("https://cdn.example/story-playback.jpg");
                        assertThat(item.musicName()).isEqualTo("Story song");
                    });
                    assertThat(page.pageNumber()).isZero();
                    assertThat(page.totalElements()).isEqualTo(1L);
                })
                .verifyComplete();
    }
}
