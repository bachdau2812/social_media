package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.user.dto.request.StoryHighlightRequest;
import com.dauducbach.clone.modules.user.dto.response.StoryHighlightResponse;
import com.dauducbach.clone.modules.user.dto.response.StoryViewerResponse;
import com.dauducbach.clone.modules.user.entity.StoryHighlight;
import com.dauducbach.clone.modules.user.entity.StoryHighlightItem;
import com.dauducbach.clone.modules.user.entity.StoryView;
import com.dauducbach.clone.modules.user.entity.UserStories;
import com.dauducbach.clone.modules.user.repositoty.StoryHighlightItemRepository;
import com.dauducbach.clone.modules.user.repositoty.StoryHighlightRepository;
import com.dauducbach.clone.modules.user.repositoty.StoryViewRepository;
import com.dauducbach.clone.modules.user.repositoty.UserStoriesRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
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
    private final DatabaseClient databaseClient;

    public Mono<Void> recordView(String storyId, String viewerId, String reaction) {
        String viewer = requireText(viewerId, "viewerId");
        return ownedStory(storyId)
                .flatMap(story -> {
                    if (viewer.equals(story.getUserId())) return Mono.empty();
                    return viewRepository.findByStoryIdAndViewerId(storyId, viewer)
                            .flatMap(existing -> {
                                if (reaction != null) existing.setReaction(normalize(reaction));
                                existing.setViewedAt(Instant.now());
                                return viewRepository.save(existing).then();
                            })
                            .switchIfEmpty(Mono.defer(() -> entityTemplate.insert(StoryView.class)
                                    .using(StoryView.builder()
                                            .id(UUID.randomUUID().toString())
                                            .storyId(storyId)
                                            .viewerId(viewer)
                                            .reaction(normalize(reaction))
                                            .viewedAt(Instant.now())
                                            .build())
                                    .then()));
                });
    }

    public Mono<PageResponse<StoryViewerResponse>> viewers(String storyId, String ownerId, int page, int size) {
        int pageNumber = Math.max(0, page);
        int pageSize = Math.max(1, Math.min(size, 50));
        int offset = pageNumber * pageSize;
        return ownedStory(storyId)
                .flatMap(story -> {
                    if (!story.getUserId().equals(ownerId)) {
                        return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Only the story owner can view viewers"));
                    }
                    String sql = """
                            SELECT sv.viewer_id, ud.username, ud.full_name, sv.reaction, sv.viewed_at,
                                   (SELECT COALESCE(NULLIF(m.secure_url, ''), m.url)
                                    FROM media m
                                    WHERE m.owner_id = sv.viewer_id AND m.owner_type = 'AVATAR'
                                    ORDER BY m.created_at DESC LIMIT 1) AS avatar_url
                            FROM story_views sv
                            LEFT JOIN user_details ud ON ud.user_id = sv.viewer_id
                            WHERE sv.story_id = :storyId
                            ORDER BY sv.viewed_at DESC
                            LIMIT :limit OFFSET :offset
                            """;
                    Mono<List<StoryViewerResponse>> content = databaseClient.sql(sql)
                            .bind("storyId", storyId)
                            .bind("limit", pageSize)
                            .bind("offset", offset)
                            .map((row, metadata) -> new StoryViewerResponse(
                                    row.get("viewer_id", String.class),
                                    row.get("username", String.class),
                                    row.get("full_name", String.class),
                                    row.get("avatar_url", String.class),
                                    row.get("reaction", String.class),
                                    row.get("viewed_at", Instant.class)
                            ))
                            .all()
                            .collectList();
                    return Mono.zip(content, viewRepository.countByStoryId(storyId).defaultIfEmpty(0L))
                            .map(result -> PageResponse.of(result.getT1(), pageNumber, result.getT2(), pageSize));
                });
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
        Mono<List<UserStories>> stories = highlightItemRepository.findByHighlightIdOrderByOrderNumberAsc(highlight.getId())
                .concatMap(item -> storiesRepository.findById(item.getStoryId()))
                .collectList();
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
