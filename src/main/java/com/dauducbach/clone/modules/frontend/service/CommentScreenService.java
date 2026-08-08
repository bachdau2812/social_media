package com.dauducbach.clone.modules.frontend.service;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.frontend.dto.CommentViewResponse;
import com.dauducbach.clone.modules.post.service.comment.CommentCompositionQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class CommentScreenService {
    private final CommentCompositionQueryService commentQueryService;

    public Mono<PageResponse<CommentViewResponse>> getRootComments(
            String postId, String viewerId, int page, int size
    ) {
        return commentQueryService.getRootComments(postId, viewerId, page, size)
                .map(source -> new PageResponse<>(
                        source.content().stream().map(this::toResponse).toList(),
                        source.pageNumber(),
                        source.totalElements(),
                        source.totalPages()
                ));
    }

    public Flux<CommentViewResponse> getReplies(
            String parentId, String viewerId, int page, int size
    ) {
        return commentQueryService.getReplies(parentId, viewerId, page, size)
                .map(this::toResponse);
    }

    private CommentViewResponse toResponse(CommentCompositionQueryService.CommentSnapshot comment) {
        return new CommentViewResponse(
                comment.id(),
                comment.postId(),
                comment.userId(),
                comment.parentId(),
                comment.content(),
                comment.commentType(),
                comment.mediaUrl(),
                comment.timestamp(),
                comment.replyCount(),
                comment.hasLiked(),
                comment.username(),
                comment.fullName(),
                comment.avatarUrl()
        );
    }
}
