package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.dto.response.PostDetailResponse;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.RichPostSearchResponse;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.user.service.MediaForProfile;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class RichPostSearchService {
    private static final int MAX_THUMBNAILS = 3;

    private final PostSearchService postSearchService;
    private final PostDetailQueryService postDetailQueryService;
    private final MediaForProfile mediaForProfile;

    public Mono<PageResponse<RichPostSearchResponse>> search(String query, int page, int limit) {
        return postSearchService.searchPosts(query, page, limit)
                .flatMap(result -> Flux.fromIterable(result.content())
                        .concatMap(this::hydrate)
                        .collectList()
                        .map(content -> new PageResponse<>(
                                content,
                                result.pageNumber(),
                                result.totalElements(),
                                result.totalPages()
                        )));
    }

    private Mono<RichPostSearchResponse> hydrate(String postId) {
        return postDetailQueryService.getPostDetail(postId, MediaDisplayType.SEARCH_THUMBNAIL)
                .flatMap(detail -> currentAvatar(detail.userId())
                        .map(avatar -> toResponse(detail, avatar)));
    }

    private Mono<String> currentAvatar(String userId) {
        return mediaForProfile.getCurrentAvatar(userId, MediaDisplayType.AVATAR)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .onErrorReturn(Optional.empty())
                .map(avatar -> avatar.map(this::avatarUrl).orElse(""));
    }

    private RichPostSearchResponse toResponse(PostDetailResponse detail, String avatarUrl) {
        List<PostItemResponse> allItems = detail.items() == null ? List.of() : detail.items();
        List<PostItemResponse> thumbnails = allItems.stream()
                .sorted(Comparator.comparing(
                        PostItemResponse::orderNumber,
                        Comparator.nullsLast(Integer::compareTo)
                ))
                .limit(MAX_THUMBNAILS)
                .toList();
        return new RichPostSearchResponse(
                detail.postId(),
                detail.userId(),
                detail.authorUsername(),
                detail.authorFullName(),
                avatarUrl,
                detail.content(),
                detail.hashtags(),
                detail.mediaRatio(),
                thumbnails,
                allItems.size(),
                detail.createdAt()
        );
    }

    private String avatarUrl(Media media) {
        if (media.getSecureUrl() != null && !media.getSecureUrl().isBlank()) {
            return media.getSecureUrl().trim();
        }
        return media.getUrl() == null ? "" : media.getUrl().trim();
    }
}
