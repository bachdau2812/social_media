package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.user.dto.response.StoryTrayResponse;
import com.dauducbach.clone.modules.user.entity.Musics;
import com.dauducbach.clone.modules.user.entity.StoryView;
import com.dauducbach.clone.modules.user.entity.UserStories;
import com.dauducbach.clone.modules.user.repositoty.MusicsRepository;
import com.dauducbach.clone.modules.user.repositoty.StoryViewRepository;
import com.dauducbach.clone.modules.user.repositoty.UserStoriesRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class StoryTrayQueryService {
    private static final int HOME_STORY_LIMIT = 20;

    private final UserStoriesRepository userStoriesRepository;
    private final MusicsRepository musicsRepository;
    private final StoryViewRepository storyViewRepository;
    private final UserDiscoveryHydrator userDiscoveryHydrator;
    private final MediaCompatibilityFacade mediaFacade;
    private final StoryMusicSegmentPolicy storyMusicSegmentPolicy;

    public Mono<List<StoryTrayResponse>> getHomeStoryTray(String viewerId) {
        Instant now = Instant.now();
        return userStoriesRepository.findHomeStoryTray(viewerId, now, HOME_STORY_LIMIT)
                .filter(story -> isActive(story, now))
                .collectList()
                .flatMap(stories -> hydrateStories(viewerId, stories));
    }

    private boolean isActive(UserStories story, Instant now) {
        Instant expiresAt = story.getExpiredAt();
        if (expiresAt != null) return expiresAt.isAfter(now);
        Instant createdAt = story.getCreatedAt();
        return createdAt != null && createdAt.plusSeconds(24 * 60 * 60).isAfter(now);
    }

    private Mono<List<StoryTrayResponse>> hydrateStories(String viewerId, List<UserStories> stories) {
        if (stories.isEmpty()) {
            return Mono.just(List.of());
        }
        List<String> storyIds = stories.stream().map(UserStories::getId).toList();
        return storyViewRepository.findByViewerIdAndStoryIdIn(viewerId, storyIds)
                .collectList()
                .map(views -> views.stream().collect(java.util.stream.Collectors.toMap(
                        StoryView::getStoryId,
                        Function.identity(),
                        (first, ignored) -> first)))
                .flatMapMany(views -> Flux.fromIterable(stories)
                        .concatMap(story -> {
                            StoryView view = views.get(story.getId());
                            boolean viewerSeen = viewerId.equals(story.getUserId()) || view != null;
                            String viewerReaction = view == null ? null : normalizeOptional(view.getReaction());
                            return toResponse(story, viewerSeen, viewerReaction);
                        }))
                .collectList();
    }

    private Mono<StoryTrayResponse> toResponse(
            UserStories story,
            boolean viewerSeen,
            String viewerReaction
    ) {
        String musicId = normalizeOptional(story.getMusicId());
        Mono<Musics> music = musicId == null
                ? Mono.just(Musics.builder().build())
                : musicsRepository.findById(musicId).defaultIfEmpty(Musics.builder().build());

        return Mono.zip(music, userDiscoveryHydrator.hydrate(story.getUserId(), story.getUserId()))
                .map(tuple -> new StoryTrayResponse(
                        story.getId(),
                        story.getUserId(),
                        tuple.getT2().username(),
                        tuple.getT2().fullName(),
                        tuple.getT2().avatarUrl(),
                        mediaFacade.transformDeliveryUrl(story.getMediaUrl(), MediaDisplayType.STORY),
                        story.getMediaType(),
                        musicId,
                        firstNonBlank(story.getMusicUrl(), tuple.getT1().getSongUrl()),
                        normalizeOptional(firstNonBlank(
                                tuple.getT1().getDisplayName(),
                                tuple.getT1().getSingleName(),
                                tuple.getT1().getSlugName())),
                        story.getMusicStart(),
                        story.getMusicEnd(),
                        storyMusicSegmentPolicy.durationSeconds(
                                story.getMediaType(),
                                story.getMusicStart(),
                                story.getMusicEnd()),
                        story.getStatus(),
                        story.getCreatedAt(),
                        story.getExpiredAt(),
                        story.getPublicationId(),
                        story.getPublicationOrder(),
                        story.getPublicationItemCount(),
                        viewerSeen,
                        viewerReaction
                ));
    }

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }
}
