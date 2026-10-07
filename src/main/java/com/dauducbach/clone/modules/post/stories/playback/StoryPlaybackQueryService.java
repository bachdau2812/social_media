package com.dauducbach.clone.modules.post.stories.playback;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.post.dto.story.response.StoryArchiveResponse;
import com.dauducbach.clone.modules.post.entity.story.StoryView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repository.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class StoryPlaybackQueryService {
    private static final Logger log = LoggerFactory.getLogger(StoryPlaybackQueryService.class);
    private static final String STATUS_APPROVED = "APPROVED";

    private final UserStoriesRepository userStoriesRepository;
    private final StoryViewRepository viewRepository;
    private final R2dbcEntityTemplate entityTemplate;
    private final MediaAssets mediaAssets;
    private final StoryPlaybackHydrator storyPlaybackHydrator;

    public Mono<PageResponse<UserStories>> getStories(String userId, int page, int size) {
        return getStories(userId, userId, page, size);
    }

    public Mono<PageResponse<UserStories>> getStories(String userId, String viewerId, int page, int size) {
        if (userId == null || userId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "userId is required"));
        }
        if (viewerId == null || viewerId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.AUTHENTICATION_FAILED, "Authenticated viewer is required"));
        }

        int pageNumber = Math.max(page, 0);
        int pageSize = Math.clamp(size, 1, 100);
        boolean owner = userId.equals(viewerId);
        long offset = (long) pageNumber * pageSize;
        Instant now = Instant.now();
        Mono<Long> total = owner
                ? userStoriesRepository.countByUserIdAndStatus(userId, STATUS_APPROVED)
                : userStoriesRepository.countActiveApprovedByUserId(userId, now);
        Flux<UserStories> content = owner
                ? userStoriesRepository.findByUserIdAndStatusOrderByCreatedAtDesc(
                        userId, STATUS_APPROVED, PageRequest.of(pageNumber, pageSize))
                : userStoriesRepository.findActiveApprovedByUserId(userId, now, pageSize, offset);
        log.info("|StoryPlaybackQueryService|getStories|userId={}|viewerId={}|owner={}|page={}|size={}",
                userId, viewerId, owner, pageNumber, pageSize);
        return total.flatMap(count -> content.collectList()
                        .flatMap(stories -> hydrateViewerSeen(stories, viewerId, owner))
                        .map(stories -> PageResponse.of(stories, pageNumber, count, pageSize)))
                .doOnError(error -> log.error("|StoryPlaybackQueryService|getStories|failed|userId={}|error={}",
                        userId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException ? error : new AppException(
                        ErrorCode.STORY_SAVE_FAILED,
                        String.format("Fetch stories failed for userId=%s", userId),
                        error));
    }

    public Mono<PageResponse<StoryArchiveResponse>> getStories(
            String userId, int page, int size, MediaDisplayType mediaType
    ) {
        return getStories(userId, userId, page, size, mediaType);
    }

    public Mono<PageResponse<StoryArchiveResponse>> getStories(
            String userId, String viewerId, int page, int size, MediaDisplayType mediaType
    ) {
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.STORY : mediaType;
        return getStories(userId, viewerId, page, size)
                .flatMap(response -> storyPlaybackHydrator.hydrateAll(
                                response.content(),
                                story -> mediaAssets.transformDeliveryUrl(story.getMediaUrl(), displayType))
                        .map(content -> new PageResponse<>(
                                content,
                                response.pageNumber(),
                                response.totalElements(),
                                response.totalPages())));
    }

    private Mono<List<UserStories>> hydrateViewerSeen(List<UserStories> stories, String viewerId, boolean owner) {
        if (stories.isEmpty()) return Mono.just(stories);
        if (owner) {
            stories.forEach(story -> story.setViewerSeen(true));
            return Mono.just(stories);
        }
        List<String> storyIds = stories.stream().map(UserStories::getId).toList();
        return entityTemplate.select(StoryView.class)
                .matching(Query.query(Criteria.where("viewerId").is(viewerId).and("storyId").in(storyIds)))
                .all()
                .map(StoryView::getStoryId)
                .collectList()
                .map(viewedIds -> {
                    stories.forEach(story -> story.setViewerSeen(viewedIds.contains(story.getId())));
                    return stories;
                });
    }
}
