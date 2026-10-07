package com.dauducbach.clone.modules.post.stories.library;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.story.request.StoryHighlightRequest;
import com.dauducbach.clone.modules.post.dto.story.response.StoryHighlightResponse;
import com.dauducbach.clone.modules.post.dto.story.response.StoryArchiveResponse;
import com.dauducbach.clone.modules.post.dto.story.response.StoryViewerResponse;
import com.dauducbach.clone.modules.post.entity.story.StoryHighlight;
import com.dauducbach.clone.modules.post.entity.story.StoryHighlightItem;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.stories.playback.StoryPlaybackHydrator;
import com.dauducbach.clone.modules.post.repository.story.StoryHighlightItemRepository;
import com.dauducbach.clone.modules.post.repository.story.StoryHighlightRepository;
import com.dauducbach.clone.modules.post.repository.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repository.story.StoryViewQueryRepository;
import com.dauducbach.clone.modules.post.repository.story.projection.StoryViewerRow;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import com.dauducbach.clone.modules.user.publicapi.UserRelationshipQuery;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class StoryLibraryService {
    private final UserStoriesRepository storiesRepository;
    private final StoryViewRepository viewRepository;
    private final StoryHighlightRepository highlightRepository;
    private final StoryHighlightItemRepository highlightItemRepository;
    private final R2dbcEntityTemplate entityTemplate;
    private final StoryViewQueryRepository storyViewQuery;
    private final UserIdentityQuery userIdentityQuery;
    private final StoryPlaybackHydrator storyPlaybackHydrator;
    private final StoryViewerSearch viewerSearch;
    private final UserRelationshipQuery relationshipQuery;

    public Mono<Void> recordView(String storyId, String viewerId, String reaction) {
        String viewer = requireText(viewerId, "viewerId");
        return ownedStory(storyId)
                .flatMap(story -> {
                    if (viewer.equals(story.getUserId())) return Mono.empty();
                    return viewRepository.upsertView(
                                    UUID.randomUUID().toString(),
                                    storyId,
                                    viewer,
                                    normalize(reaction),
                                    Instant.now())
                            .then();
                });
    }

    public Mono<PageResponse<StoryViewerResponse>> viewers(String storyId, String ownerId, int page, int size) {
        return viewers(storyId, ownerId, page, size, "");
    }

    public Mono<PageResponse<StoryViewerResponse>> viewers(String storyId, String ownerId, int page, int size, String query) {
        int pageNumber = Math.max(0, page);
        int pageSize = Math.max(1, Math.min(size, 50));
        int offset = Math.multiplyExact(pageNumber, pageSize);
        String search = normalize(query);
        return ownedStory(storyId).flatMap(story -> {
            if (!story.getUserId().equals(ownerId)) {
                return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Only the story owner can view viewers"));
            }
            Mono<PageResponse<StoryViewerRow>> rows = search == null
                    ? Mono.zip(storyViewQuery.findViewerPage(storyId, pageSize, offset).collectList(),
                            viewRepository.countByStoryId(storyId).defaultIfEmpty(0L))
                        .map(result -> PageResponse.of(result.getT1(), pageNumber, result.getT2(), pageSize))
                    : viewerSearch.search(storyId, search, pageNumber, pageSize, offset);
            return rows.flatMap(result -> {
                var ids = result.content().stream().map(StoryViewerRow::viewerId).toList();
                if (ids.isEmpty()) return Mono.just(new PageResponse<StoryViewerResponse>(
                        List.of(), result.pageNumber(), result.totalElements(), result.totalPages()));
                return Mono.zip(userIdentityQuery.findIdentities(ids).collectMap(UserIdentity::userId),
                                relationshipQuery.findFollowingIds(ownerId, ids).collectList())
                        .map(data -> new PageResponse<>(result.content().stream()
                                .map(row -> toViewerResponse(row, data.getT1().get(row.viewerId()),
                                        data.getT2().contains(row.viewerId())))
                                .toList(), result.pageNumber(), result.totalElements(), result.totalPages()));
            });
        });
    }

    private StoryViewerResponse toViewerResponse(StoryViewerRow row, UserIdentity identity, boolean following) {
        return new StoryViewerResponse(row.viewerId(),
                identity == null ? null : identity.username(),
                identity == null ? null : identity.fullName(),
                identity == null ? null : identity.avatarUrl(),
                row.reaction(), row.viewedAt(), following);
    }

    public Mono<Void> deleteStory(String storyId, String authenticatedUserId) {
        String ownerId = requireText(authenticatedUserId, "authenticatedUserId");
        return ownedStory(storyId)
                .flatMap(story -> {
                    if (!ownerId.equals(story.getUserId())) {
                        return Mono.error(new AppException(ErrorCode.AUTHENTICATION_FAILED, "Only the story owner can delete it"));
                    }
                    story.setStatus("REMOVED");
                    return storiesRepository.save(story).then();
                });
    }

    public Mono<StoryHighlightResponse> createHighlight(StoryHighlightRequest request) {
        String ownerId = requireText(request.ownerId(), "ownerId");
        String title = requireText(request.title(), "title");
        List<String> storyIds = request.storyIds() == null ? List.of() : request.storyIds().stream().distinct().toList();
        return validateOwnedStories(ownerId, storyIds)
                .then(Mono.defer(() -> {
                    Instant now = Instant.now();
                    StoryHighlight highlight = StoryHighlight.builder()
                            .id(UUID.randomUUID().toString())
                            .ownerId(ownerId)
                            .title(title)
                            .coverStoryId(normalize(request.coverStoryId()))
                            .createdAt(now)
                            .updatedAt(now)
                            .build();
                    return entityTemplate.insert(StoryHighlight.class).using(highlight)
                            .flatMap(saved -> insertHighlightItems(saved.getId(), storyIds)
                                    .then(hydrateHighlight(saved)));
                }));
    }

    public Flux<StoryHighlightResponse> listHighlights(String ownerId) {
        return highlightRepository.findByOwnerIdOrderByUpdatedAtDesc(requireText(ownerId, "ownerId"))
                .concatMap(this::hydrateHighlight);
    }

    public Mono<StoryHighlightResponse> updateHighlight(String highlightId, StoryHighlightRequest request) {
        return highlightRepository.findById(highlightId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Highlight not found")))
                .flatMap(highlight -> {
                    if (!highlight.getOwnerId().equals(request.ownerId())) {
                        return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Only the highlight owner can update it"));
                    }
                    List<String> storyIds = request.storyIds() == null ? List.of() : request.storyIds().stream().distinct().toList();
                    return validateOwnedStories(highlight.getOwnerId(), storyIds)
                            .then(Mono.defer(() -> {
                                highlight.setTitle(requireText(request.title(), "title"));
                                highlight.setCoverStoryId(normalize(request.coverStoryId()));
                                highlight.setUpdatedAt(Instant.now());
                                return highlightRepository.save(highlight)
                                        .flatMap(saved -> highlightItemRepository.deleteByHighlightId(saved.getId())
                                                .then(insertHighlightItems(saved.getId(), storyIds))
                                                .then(hydrateHighlight(saved)));
                            }));
                });
    }

    public Mono<Void> deleteHighlight(String highlightId, String ownerId) {
        return highlightRepository.findById(highlightId)
                .filter(highlight -> highlight.getOwnerId().equals(ownerId))
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Highlight not found")))
                .flatMap(highlight -> highlightRepository.deleteById(highlight.getId()));
    }

    private Mono<Void> validateOwnedStories(String ownerId, List<String> storyIds) {
        return Flux.fromIterable(storyIds)
                .concatMap(storiesRepository::findById)
                .filter(story -> ownerId.equals(story.getUserId()))
                .count()
                .flatMap(count -> count == storyIds.size()
                        ? Mono.empty()
                        : Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Highlight contains an unavailable story")));
    }

    private Mono<Void> insertHighlightItems(String highlightId, List<String> storyIds) {
        return Flux.range(0, storyIds.size())
                .concatMap(index -> entityTemplate.insert(StoryHighlightItem.class)
                        .using(StoryHighlightItem.builder()
                                .id(UUID.randomUUID().toString())
                                .highlightId(highlightId)
                                .storyId(storyIds.get(index))
                                .orderNumber(index + 1)
                                .createdAt(Instant.now())
                                .build()))
                .then();
    }

    private Mono<StoryHighlightResponse> hydrateHighlight(StoryHighlight highlight) {
        Mono<List<StoryArchiveResponse>> stories = highlightItemRepository.findByHighlightIdOrderByOrderNumberAsc(highlight.getId())
                .concatMap(item -> storiesRepository.findById(item.getStoryId()))
                .collectList()
                .flatMap(items -> storyPlaybackHydrator.hydrateAll(items, UserStories::getMediaUrl));
        Mono<String> cover = normalize(highlight.getCoverStoryId()) == null
                ? Mono.just("")
                : storiesRepository.findById(highlight.getCoverStoryId()).map(UserStories::getMediaUrl).defaultIfEmpty("");
        return Mono.zip(stories, cover)
                .map(result -> new StoryHighlightResponse(
                        highlight.getId(), highlight.getOwnerId(), highlight.getTitle(), highlight.getCoverStoryId(),
                        result.getT2(), highlight.getCreatedAt(), highlight.getUpdatedAt(), result.getT1()
                ));
    }

    private Mono<UserStories> ownedStory(String storyId) {
        return storiesRepository.findById(requireText(storyId, "storyId"))
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Story not found")));
    }

    private String requireText(String value, String field) {
        String normalized = normalize(value);
        if (normalized == null) throw new AppException(ErrorCode.STORY_SAVE_FAILED, field + " is required");
        return normalized;
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
