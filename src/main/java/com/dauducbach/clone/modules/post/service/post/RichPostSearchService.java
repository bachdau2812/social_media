package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.dto.response.PostDetailResponse;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.RichPostSearchResponse;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
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
    private final UserIdentityQuery userIdentityQuery;

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
                .flatMap(detail -> userIdentityQuery.findIdentity(detail.userId())
                        .map(identity -> toResponse(detail, identity.avatarUrl()))
                        .defaultIfEmpty(toResponse(detail, ""))
                        .onErrorReturn(toResponse(detail, "")));
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

}
