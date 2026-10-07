package com.dauducbach.clone.modules.post.stories.query;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.publicapi.MediaUrlDelivery;
import com.dauducbach.clone.modules.post.dto.story.response.StoryTrayResponse;
import com.dauducbach.clone.modules.post.dto.story.response.StoryArchiveResponse;
import com.dauducbach.clone.modules.post.entity.story.StoryView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.publicapi.StoryTrayQuery;
import com.dauducbach.clone.modules.post.stories.policy.StoryExpiryPolicy;
import com.dauducbach.clone.modules.post.stories.playback.StoryPlaybackHydrator;
import com.dauducbach.clone.modules.post.repository.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Function;
import java.time.Instant;

@Service
@RequiredArgsConstructor
public class StoryTrayQueryService implements StoryTrayQuery {
    private static final int HOME_STORY_LIMIT = 20;

    private final UserStoriesRepository userStoriesRepository;
    private final StoryViewRepository storyViewRepository;
    private final UserDiscoveryQuery userDiscoveryQuery;
    private final MediaUrlDelivery mediaAssets;
    private final StoryPlaybackHydrator storyPlaybackHydrator;

    @Override
    public Mono<List<StoryTrayQuery.StoryTraySnapshot>> getHomeStoryTray(String viewerId) {
        Instant now = Instant.now();
        return userStoriesRepository.findHomeStoryTray(viewerId, now, HOME_STORY_LIMIT)
                .filter(story -> StoryExpiryPolicy.isActive(story, now))
                .collectList()
                .flatMap(stories -> hydrateStories(viewerId, stories))
                .map(stories -> stories.stream().map(this::toSnapshot).toList());
    }

    private StoryTrayQuery.StoryTraySnapshot toSnapshot(StoryTrayResponse story) {
        return new StoryTrayQuery.StoryTraySnapshot(
                story.storyId(), story.userId(), story.username(), story.fullName(), story.avatarUrl(),
                story.mediaUrl(), story.mediaType(), story.musicId(), story.musicUrl(), story.musicDisplayName(),
                story.musicStart(), story.musicEnd(), story.durationSeconds(), story.status(), story.createdAt(),
                story.expiredAt(), story.publicationId(), story.publicationOrder(), story.publicationItemCount(),
                story.viewerSeen(), story.viewerReaction());
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
                .flatMapMany(views -> storyPlaybackHydrator.hydrateAll(
                                stories,
                                story -> mediaAssets.transformDeliveryUrl(story.getMediaUrl(), MediaDisplayType.STORY))
                        .flatMapMany(playbackItems -> Flux.fromIterable(playbackItems)
                        .concatMap(playback -> {
                            StoryView view = views.get(playback.id());
                            boolean viewerSeen = viewerId.equals(playback.userId()) || view != null;
                            String viewerReaction = view == null ? null : normalizeOptional(view.getReaction());
                            return toResponse(playback, viewerSeen, viewerReaction);
                        })))
                .collectList();
    }

    private Mono<StoryTrayResponse> toResponse(
            StoryArchiveResponse story,
            boolean viewerSeen,
            String viewerReaction
    ) {
        return userDiscoveryQuery.hydrate(story.userId(), story.userId())
                .map(identity -> new StoryTrayResponse(
                        story.id(),
                        story.userId(),
                        identity.username(),
                        identity.fullName(),
                        identity.avatarUrl(),
                        story.mediaUrl(),
                        story.mediaType(),
                        story.musicId(),
                        story.musicUrl(),
                        story.musicName(),
                        story.musicStart(),
                        story.musicEnd(),
                        story.durationSeconds(),
                        story.status(),
                        story.createdAt(),
                        story.expiredAt(),
                        story.publicationId(),
                        story.publicationOrder(),
                        story.publicationItemCount(),
                        viewerSeen,
                        viewerReaction
                ));
    }

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

}
