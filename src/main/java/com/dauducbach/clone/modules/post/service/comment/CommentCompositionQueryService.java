package com.dauducbach.clone.modules.post.service.comment;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.user.service.UserIdentityQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CommentCompositionQueryService {
    private final CommentService commentService;
    private final UserIdentityQueryService userIdentityQueryService;

    public Mono<PageResponse<CommentSnapshot>> getRootComments(
            String postId, String viewerId, int page, int size
    ) {
        Mono<PageResponse<Comment>> source = viewerId == null || viewerId.isBlank()
                ? commentService.getRootCommentsPage(postId, page, size)
                : commentService.getRootCommentsPage(postId, viewerId, page, size);
        return source.flatMap(this::enrichPage);
    }

    public Flux<CommentSnapshot> getReplies(String parentId, String viewerId, int page, int size) {
        Flux<Comment> source = viewerId == null || viewerId.isBlank()
                ? commentService.getChildComments(parentId, page, size)
                : commentService.getChildComments(parentId, viewerId, page, size);
        return source.concatMap(this::enrichComment);
    }

    private Mono<PageResponse<CommentSnapshot>> enrichPage(PageResponse<Comment> page) {
        return Flux.fromIterable(page.content() == null ? List.of() : page.content())
                .concatMap(this::enrichComment)
                .collectList()
                .map(content -> new PageResponse<>(
                        content, page.pageNumber(), page.totalElements(), page.totalPages()));
    }

    private Mono<CommentSnapshot> enrichComment(Comment comment) {
        UserIdentityQueryService.IdentitySnapshot fallback =
                new UserIdentityQueryService.IdentitySnapshot(
                        comment.getUserId(), "Unknown user", "Unknown user", "");
        return userIdentityQueryService.findIdentity(comment.getUserId())
                .defaultIfEmpty(fallback)
                .onErrorReturn(fallback)
                .map(user -> new CommentSnapshot(
                        comment.getId(),
                        comment.getPostId(),
                        comment.getUserId(),
                        comment.getParentId(),
                        comment.getContent(),
                        comment.getCommentType(),
                        comment.getMediaUrl(),
                        comment.getTimestamp(),
                        comment.getReplyCount(),
                        comment.isHasLiked(),
                        firstNonBlank(user.username(), "Unknown user"),
                        firstNonBlank(user.fullName(), user.username(), "Unknown user"),
                        firstNonBlank(user.avatarUrl())
                ));
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }

    public record CommentSnapshot(
            String id,
            String postId,
            String userId,
            String parentId,
            String content,
            String commentType,
            String mediaUrl,
            Instant timestamp,
            long replyCount,
            boolean hasLiked,
            String username,
            String fullName,
            String avatarUrl
    ) {
    }
}

