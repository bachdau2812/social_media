package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.service.music.MusicPlaybackCatalog;
import com.dauducbach.clone.modules.post.dto.story.response.StoryArchiveResponse;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryPlaybackHydratorTest {
    @Mock MusicPlaybackCatalog musicPlaybackCatalog;

    private StoryPlaybackHydrator hydrator;

    @BeforeEach
    void setUp() {
        hydrator = new StoryPlaybackHydrator(musicPlaybackCatalog, new StoryMusicSegmentPolicy());
    }

    @Test
    void resolvesCatalogMusicAndPreservesStoryOrder() {
        when(musicPlaybackCatalog.findAllByIds(any(Iterable.class))).thenReturn(Flux.just(
                music("music-2", "Second", "/two.mp3", true),
                music("music-1", "First", "/one.mp3", true)));

        StepVerifier.create(hydrator.hydrateAll(
                        List.of(story("story-1", "music-1", null), story("story-2", "music-2", null)),
                        story -> "/display/" + story.getId()))
                .assertNext(items -> {
                    assertThat(items).extracting(StoryArchiveResponse::id)
                            .containsExactly("story-1", "story-2");
                    assertThat(items.getFirst().mediaUrl()).isEqualTo("/display/story-1");
                    assertThat(items.getFirst().musicUrl()).isEqualTo("/one.mp3");
                    assertThat(items.getFirst().musicName()).isEqualTo("First");
                    assertThat(items.getFirst().durationSeconds()).isEqualTo(30L);
                })
                .verifyComplete();
    }

    @Test
    void prefersPersistedStoryMusicUrl() {
        when(musicPlaybackCatalog.findAllByIds(any(Iterable.class)))
                .thenReturn(Flux.just(music("music-1", "First", "/catalog.mp3", true)));

        StepVerifier.create(hydrator.hydrate(story("story-1", "music-1", "/persisted.mp3"), UserStories::getMediaUrl))
                .assertNext(item -> assertThat(item.musicUrl()).isEqualTo("/persisted.mp3"))
                .verifyComplete();
    }

    @Test
    void missingOrUnfetchedMusicFallsBackToSilentStory() {
        when(musicPlaybackCatalog.findAllByIds(any(Iterable.class))).thenReturn(Flux.just(
                music("music-unfetched", "Pending", null, false)));

        StepVerifier.create(hydrator.hydrateAll(
                        List.of(
                                story("missing", "music-missing", null),
                                story("unfetched", "music-unfetched", null),
                                story("silent", null, null)),
                        UserStories::getMediaUrl))
                .assertNext(items -> {
                    assertThat(items).extracting(StoryArchiveResponse::musicUrl)
                            .containsExactly(null, null, null);
                    assertThat(items).extracting(StoryArchiveResponse::durationSeconds)
                            .containsExactly(5L, 5L, 5L);
                })
                .verifyComplete();
    }

    private UserStories story(String id, String musicId, String musicUrl) {
        return UserStories.builder()
                .id(id)
                .userId("owner-1")
                .mediaUrl("/media/" + id)
                .mediaType("IMAGE")
                .musicId(musicId)
                .musicUrl(musicUrl)
                .musicStart(5L)
                .musicEnd(35L)
                .publicationId("publication-1")
                .publicationOrder(1)
                .publicationItemCount(1)
                .status("APPROVED")
                .createdAt(Instant.parse("2026-08-10T00:00:00Z"))
                .expiredAt(Instant.parse("2026-08-11T00:00:00Z"))
                .viewerSeen(true)
                .build();
    }

    private Musics music(String id, String displayName, String songUrl, boolean fetched) {
        return Musics.builder()
                .id(id)
                .displayName(displayName)
                .singleName("Artist")
                .songUrl(songUrl)
                .fetched(fetched)
                .build();
    }
}
