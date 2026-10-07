package com.dauducbach.clone.modules.post.comments.query;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.publicapi.MediaUrlDelivery;
import com.dauducbach.clone.modules.post.comments.application.CommentCountCache;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.entity.Like;
import com.dauducbach.clone.modules.post.repository.CommentRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class CommentReadService {
    private static final Logger log = LoggerFactory.getLogger(CommentReadService.class);
    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final int MAX_PAGE_SIZE = 100;

    private final CommentRepository commentRepository;
    private final CommentCountCache commentCountCache;
    private final R2dbcEntityTemplate r2dbcEntityTemplate;
    private final MediaUrlDelivery mediaAssets;

    public Flux<Comment> getRootComments(String postId, int page, int size) {
        return getRootComments(postId, null, page, size);
    }

    public Flux<Comment> getRootComments(String postId, String viewerId, int page, int size) {
        validatePostId(postId);
        int limit = validatePageSize(size);
        int offset = normalizePage(page) * limit;

        log.info("|CommentReadService|getRootComments|postId={}|viewerId={}|page={}|size={}", postId, viewerId, page, size);
        return commentRepository.findRootByPostId(postId, limit, offset)
                .concatMap(comment -> enrichCommentForViewer(comment, viewerId))
                .onErrorMap(error -> new AppException(
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Fetch root comments failed for postId=%s", postId),
                        error
                ));
    }

    public Mono<PageResponse<Comment>> getRootCommentsPage(String postId, int page, int size) {
        return getRootCommentsPage(postId, null, page, size);
    }

    public Mono<PageResponse<Comment>> getRootCommentsPage(String postId, String viewerId, int page, int size) {
        validatePostId(postId);
        int pageNumber = normalizePage(page);
        int pageSize = validatePageSize(size);
        return commentRepository.countRootByPostId(postId)
                .flatMap(total -> getRootComments(postId, viewerId, pageNumber, pageSize)
                        .collectList()
                        .map(content -> PageResponse.of(content, pageNumber, total, pageSize)))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.COMMENT_FETCH_FAILED, "Fetch root comment page failed", error));
    }

    public Flux<Comment> getChildComments(String parentId, int page, int size) {
        return getChildComments(parentId, null, page, size);
    }

    public Flux<Comment> getChildComments(String parentId, String viewerId, int page, int size) {
        validateCommentId(parentId);
        int limit = validatePageSize(size);
        int offset = normalizePage(page) * limit;

        log.info("|CommentReadService|getChildComments|parentId={}|viewerId={}|page={}|size={}", parentId, viewerId, page, size);
        return commentRepository.findByParentId(parentId, limit, offset)
                .concatMap(comment -> enrichCommentForViewer(comment, viewerId))
                .onErrorMap(error -> new AppException(
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Fetch child comments failed for parentId=%s", parentId),
                        error
                ));
    }

    private Mono<Comment> enrichCommentForViewer(Comment comment, String viewerId) {
        Mono<Long> replyCount = commentRepository.countByParentId(comment.getId()).defaultIfEmpty(0L);
        Mono<Boolean> hasLiked = viewerId == null || viewerId.isBlank()
                ? Mono.just(false)
                : r2dbcEntityTemplate.exists(
                        Query.query(Criteria.where("actorId").is(viewerId)
                                .and("targetId").is(comment.getId())
                                .and("targetType").is(EntityType.COMMENT.name())),
                        Like.class
                ).defaultIfEmpty(false);

        return Mono.zip(replyCount, hasLiked)
                .map(state -> {
                    comment.setReplyCount(state.getT1());
                    comment.setHasLiked(state.getT2());
                    return transformCommentMedia(comment);
                });
    }

    public Mono<Comment> getCommentById(String commentId) {
        log.info("|CommentReadService|getCommentById|commentId={}", commentId);
        return commentRepository.findById(commentId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.COMMENT_NOT_FOUND,
                        String.format("Comment not found for commentId=%s", commentId)
                )))
                .map(this::transformCommentMedia)
                .doOnSuccess(comment -> log.info("|CommentReadService|getCommentById|success|commentId={}", commentId))
                .onErrorMap(error -> wrapFetchCommentError(commentId, error))
                .doOnError(error -> log.error("|CommentReadService|getCommentById|failed|commentId={}|error={}", commentId, error.getMessage()));
    }

    public Mono<PageResponse<String>> getCommentedPostIdsByUserId(String userId, int page, int size) {
        validateUserId(userId);
        int pageNumber = normalizePage(page);
        int pageSize = validatePageSize(size);
        int offset = pageNumber * pageSize;

        log.info("|CommentReadService|getCommentedPostIdsByUserId|userId={}|page={}|size={}", userId, pageNumber, pageSize);
        return commentRepository.countCommentedPostsByUserId(userId)
                .flatMap(totalElements -> commentRepository.findCommentedPostIdsByUserId(userId, pageSize, offset)
                        .collectList()
                        .map(content -> PageResponse.of(content, pageNumber, totalElements, pageSize)))
                .onErrorMap(error -> new AppException(
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Fetch commented posts failed for userId=%s", userId),
                        error
                ));
    }

    public Flux<String> getDistinctCommenterUserIdsByPostId(String postId) {
        validatePostId(postId);
        log.info("|CommentReadService|getDistinctCommenterUserIdsByPostId|postId={}", postId);
        return commentRepository.findDistinctUserIdsByPostId(postId)
                .filter(userId -> userId != null && !userId.isBlank())
                .distinct()
                .onErrorMap(error -> new AppException(
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Fetch commenter user ids failed for postId=%s", postId),
                        error
                ));
    }

    public Mono<PageResponse<Comment>> getCommentsByUserId(String userId, int page, int size) {
        validateUserId(userId);
        int pageNumber = normalizePage(page);
        int pageSize = validatePageSize(size);
        int offset = pageNumber * pageSize;

        log.info("|CommentReadService|getCommentsByUserId|userId={}|page={}|size={}", userId, pageNumber, pageSize);
        return commentRepository.countByUserId(userId)
                .flatMap(totalElements -> commentRepository.findByUserId(userId, pageSize, offset)
                        .map(this::transformCommentMedia)
                        .collectList()
                        .map(content -> PageResponse.of(content, pageNumber, totalElements, pageSize)))
                .onErrorMap(error -> new AppException(
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Fetch comments failed for userId=%s", userId),
                        error
                ));
    }

    public Mono<Long> countCommentsByPostId(String postId) {
        validatePostId(postId);
        log.info("|CommentReadService|countCommentsByPostId|postId={}", postId);
        return commentCountCache.getPostCount(postId, () -> commentRepository.countByPostId(postId))
                .onErrorMap(error -> new AppException(
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Count comments failed for postId=%s", postId),
                        error
                ));
    }

    public Mono<Long> countRepliesByParentId(String parentId) {
        validateCommentId(parentId);
        log.info("|CommentReadService|countRepliesByParentId|parentId={}", parentId);
        return commentRepository.countByParentId(parentId)
                .onErrorMap(error -> new AppException(
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Count replies failed for parentId=%s", parentId),
                        error
                ));
    }

    private Comment transformCommentMedia(Comment comment) {
        if (comment.getMediaUrl() != null && !comment.getMediaUrl().isBlank()) {
            comment.setMediaUrl(mediaAssets.transformDeliveryUrl(comment.getMediaUrl(), MediaDisplayType.COMMENT));
        }
        return comment;
    }

    private void validatePostId(String postId) {
        if (postId == null || postId.isBlank()) {
            throw new AppException(ErrorCode.COMMENT_FETCH_FAILED, "postId is required");
        }
    }

    private void validateCommentId(String commentId) {
        if (commentId == null || commentId.isBlank()) {
            throw new AppException(ErrorCode.COMMENT_FETCH_FAILED, "commentId is required");
        }
    }

    private void validateUserId(String userId) {
        if (userId == null || userId.isBlank()) {
            throw new AppException(ErrorCode.COMMENT_FETCH_FAILED, "userId is required");
        }
    }

    private int normalizePage(int page) {
        return Math.max(page, 0);
    }

    private int validatePageSize(int size) {
        if (size <= 0) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(size, MAX_PAGE_SIZE);
    }

    private Throwable wrapFetchCommentError(String commentId, Throwable error) {
        if (error instanceof AppException) {
            return error;
        }
        return new AppException(
                ErrorCode.COMMENT_FETCH_FAILED,
                String.format("Fetch comment failed for commentId=%s", commentId),
                error
        );
    }
}
