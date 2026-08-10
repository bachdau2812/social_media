package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repositoty.music.MusicsRepository;
import com.dauducbach.clone.modules.post.dto.story.response.StoryArchiveResponse;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class StoryPlaybackHydrator {
    private final MusicsRepository musicsRepository;
    private final StoryMusicSegmentPolicy segmentPolicy;

    public Mono<StoryArchiveResponse> hydrate(
            UserStories story,
            Function<UserStories, String> mediaUrlResolver
    ) {
        return hydrateAll(List.of(story), mediaUrlResolver).map(List::getFirst);
    }

    public Mono<List<StoryArchiveResponse>> hydrateAll(
            List<UserStories> stories,
            Function<UserStories, String> mediaUrlResolver
    ) {
        if (stories == null || stories.isEmpty()) {
            return Mono.just(List.of());
        }
        LinkedHashSet<String> musicIds = stories.stream()
                .map(UserStories::getMusicId)
                .map(this::normalize)
                .filter(value -> value != null)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Flux<Musics> catalog = musicIds.isEmpty()
                ? Flux.empty()
                : musicsRepository.findAllById(musicIds);
        return catalog.collectMap(Musics::getId, Function.identity())
                .map(musicById -> stories.stream()
                        .map(story -> toResponse(story, musicById, mediaUrlResolver))
                        .toList());
    }

    private StoryArchiveResponse toResponse(
            UserStories story,
            Map<String, Musics> musicById,
            Function<UserStories, String> mediaUrlResolver
    ) {
        String musicId = normalize(story.getMusicId());
        Musics music = musicId == null ? null : musicById.get(musicId);
        String catalogUrl = music != null && Boolean.TRUE.equals(music.getFetched())
                ? normalize(music.getSongUrl())
                : null;
        return new StoryArchiveResponse(
                story.getId(),
                story.getUserId(),
                mediaUrlResolver.apply(story),
                story.getMediaType(),
                musicId,
                firstNonBlank(story.getMusicUrl(), catalogUrl),
                musicName(music),
                story.getMusicStart(),
                story.getMusicEnd(),
                segmentPolicy.durationSeconds(
                        story.getMediaType(),
                        hasPlayableMusic(story, catalogUrl) ? story.getMusicStart() : null,
                        hasPlayableMusic(story, catalogUrl) ? story.getMusicEnd() : null),
                story.getPublicationId(),
                story.getPublicationOrder(),
                story.getPublicationItemCount(),
                story.getStatus(),
                story.getCreatedAt(),
                story.getExpiredAt(),
                story.getViewerSeen());
    }

    private boolean hasPlayableMusic(UserStories story, String catalogUrl) {
        return normalize(story.getMusicUrl()) != null || catalogUrl != null;
    }

    private String musicName(Musics music) {
        if (music == null) {
            return null;
        }
        return firstNonBlank(music.getDisplayName(), music.getSingleName(), music.getSlugName());
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            String normalized = normalize(value);
            if (normalized != null) {
                return normalized;
            }
        }
        return null;
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
