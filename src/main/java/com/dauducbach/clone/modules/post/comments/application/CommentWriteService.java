package com.dauducbach.clone.modules.post.comments.application;

import com.dauducbach.clone.commons.realtime.UserSsePublisher;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.dto.request.CommentCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.CommentUpdateRequest;
import com.dauducbach.clone.modules.post.dto.response.CommentCreateResponse;
import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.repository.CommentRepository;
import com.google.gson.JsonObject;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class CommentWriteService {
    private static final Logger log = LoggerFactory.getLogger(CommentWriteService.class);
    private static final int MIN_CONTENT_LENGTH = 1;
    private static final int MAX_CONTENT_LENGTH = 2000;
    private static final Set<String> BANNED_WORDS = Set.of("badword1", "badword2");

    CommentRepository commentRepository;
    CommentCountCache commentCountCache;
    CommentCommitter commentCommitter;
    CommentUploadWaitStore commentUploadWaitStore;
    UserSsePublisher userSsePublisher;

    public Mono<CommentCreateResponse> createComment(CommentCreateRequest request) {
        log.info("|CommentWriteService|createComment|start|postId={}|userId={}", request.getPostId(), request.getUserId());

        validateCreateRequest(request);
        List<MediaUploadRequest> mediaList = request.getMediaList() == null ? List.of() : request.getMediaList();
        boolean hasMedia = !mediaList.isEmpty();
        validateMediaList(mediaList);
        if (!hasMedia || (request.getContent() != null && !request.getContent().isBlank())) {
            validateContent(request.getContent());
        }

        String commentId = UUID.randomUUID().toString();
        Comment comment = Comment.builder()
                .id(commentId)
                .postId(request.getPostId())
                .userId(request.getUserId())
                .parentId(request.getParentId())
                .content(request.getContent())
                .commentType(hasMedia ? "MEDIA" : "TEXT")
                .moderationStatus(hasMedia ? "PENDING" : "APPROVED")
                .mediaUrl(hasMedia ? mediaList.get(0).getSecureUrl() : null)
                .timestamp(Instant.now())
                .build();
        log.info("|CommentWriteService|createComment|comment={}", comment);

        Mono<Void> waitKeyWrite = Mono.defer(() -> hasMedia
                ? commentUploadWaitStore.markWaitingForUpload(commentId, request.getUserId())
                : Mono.empty());

        return validateParent(request)
                .then(Mono.defer(() -> hasMedia
                        ? commentCommitter.savePendingMedia(comment, mediaList)
                        : commentCommitter.saveApproved(comment)))
                .doOnSuccess(comment1 -> log.info("|CommentWriteService|createComment|insert_success={}", comment1.getId()))
                .doOnError(throwable -> log.error("|CommentWriteService|createComment|insert_error|postId={}|userId={}|error={}",
                        request.getPostId(), request.getUserId(), throwable.getMessage(), throwable))
                .flatMap(saved -> {
                    Mono<Void> postSaveAction = hasMedia
                            ? waitKeyWrite
                            : sendImmediateCommentSuccess(saved);
                    return commentCountCache.invalidatePostCount(saved.getPostId())
                            .then(postSaveAction.onErrorResume(error -> {
                                log.warn("Committed comment; delivery side effect failed for {}", saved.getId(), error);
                                return Mono.empty();
                            }))
                            .thenReturn(CommentCreateResponse.builder()
                            .commentId(saved.getId())
                            .message(hasMedia ? "Dang doi xu ly va duyet media" : "Comment created")
                            .build());
                })
                .doOnSuccess(response -> log.info("|CommentWriteService|createComment|success|commentId={}", response.getCommentId()))
                .onErrorResume(error -> {
                    log.error("|CommentWriteService|createComment|failed|postId={}|userId={}|error={}",
                            request.getPostId(), request.getUserId(), error.getMessage());
                    Mono<Void> failureNotification = hasMedia
                            ? Mono.empty()
                            : sendTextCommentFailureSse(comment, "Create comment failed");
                    return failureNotification.then(Mono.error(wrapCreateError(request, error)));
                });
    }

    public Mono<Comment> updateComment(CommentUpdateRequest request) {
        log.info("|CommentWriteService|updateComment|start|commentId={}|userId={}", request.getCommentId(), request.getUserId());

        if (request.getCommentId() == null || request.getCommentId().isBlank()) {
            return Mono.error(new AppException(ErrorCode.COMMENT_UPDATE_FAILED, "commentId is required"));
        }
        if (request.getUserId() == null || request.getUserId().isBlank()) {
            return Mono.error(new AppException(ErrorCode.COMMENT_UPDATE_FAILED, "userId is required"));
        }
        validateContent(request.getContent());

        return commentRepository.findById(request.getCommentId())
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.COMMENT_NOT_FOUND,
                        String.format("Comment not found for commentId=%s", request.getCommentId())
                )))
                .flatMap(existing -> {
                    if (!request.getUserId().equals(existing.getUserId())) {
                        return Mono.error(new AppException(
                                ErrorCode.COMMENT_FORBIDDEN,
                                String.format("User %s is not owner of commentId=%s", request.getUserId(), request.getCommentId())
                        ));
                    }
                    existing.setContent(request.getContent());
                    return commentRepository.save(existing);
                })
                .doOnSuccess(updated -> log.info("|CommentWriteService|updateComment|success|commentId={}", updated.getId()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.COMMENT_UPDATE_FAILED,
                                String.format("Update comment failed for commentId=%s", request.getCommentId()),
                                error
                        ));
    }

    public Mono<Void> deleteComment(String commentId, String userId) {
        log.info("|CommentWriteService|deleteComment|start|commentId={}|userId={}", commentId, userId);

        return commentRepository.findById(commentId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.COMMENT_NOT_FOUND,
                        String.format("Comment not found for commentId=%s", commentId)
                )))
                .flatMap(existing -> {
                    if (!userId.equals(existing.getUserId())) {
                        return Mono.error(new AppException(
                                ErrorCode.COMMENT_FORBIDDEN,
                                String.format("User %s is not owner of commentId=%s", userId, commentId)
                        ));
                    }
                    return commentCountCache.getPostCount(existing.getPostId(),
                                    () -> commentRepository.countByPostId(existing.getPostId()))
                            .then()
                            .then(commentRepository.deleteById(commentId))
                            .then(commentCountCache.adjustPostCount(existing.getPostId(), -1));
                })
                .doOnSuccess(unused -> log.info("|CommentWriteService|deleteComment|success|commentId={}|userId={}", commentId, userId))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.COMMENT_DELETE_FAILED,
                                String.format("Delete comment failed for commentId=%s", commentId),
                                error
                        ));
    }

    private Mono<Void> validateParent(CommentCreateRequest request) {
        String parentId = request.getParentId();
        if (parentId == null || parentId.isBlank()) {
            return Mono.empty();
        }
        return commentRepository.findById(parentId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.COMMENT_NOT_FOUND,
                        String.format("Parent comment not found for commentId=%s", parentId)
                )))
                .flatMap(parent -> {
                    if (!request.getPostId().equals(parent.getPostId())) {
                        return Mono.error(new AppException(
                                ErrorCode.COMMENT_CREATE_FAILED,
                                "Parent comment belongs to another post"
                        ));
                    }
                    if (parent.getParentId() == null || parent.getParentId().isBlank()) {
                        return Mono.empty();
                    }
                    return commentRepository.findById(parent.getParentId())
                            .switchIfEmpty(Mono.error(new AppException(
                                    ErrorCode.COMMENT_NOT_FOUND,
                                    String.format("Parent comment chain is invalid for commentId=%s", parentId)
                            )))
                            .flatMap(grandParent -> grandParent.getParentId() != null && !grandParent.getParentId().isBlank()
                                    ? Mono.error(new AppException(
                                            ErrorCode.COMMENT_CREATE_FAILED,
                                            "Comments support at most three levels"
                                    ))
                                    : Mono.empty());
                });
    }
    private void validateCreateRequest(CommentCreateRequest request) {
        if (request == null) {
            throw new AppException(ErrorCode.COMMENT_CREATE_FAILED, "Request is required");
        }
        if (request.getPostId() == null || request.getPostId().isBlank()) {
            throw new AppException(ErrorCode.COMMENT_CREATE_FAILED, "postId is required");
        }
        if (request.getUserId() == null || request.getUserId().isBlank()) {
            throw new AppException(ErrorCode.COMMENT_CREATE_FAILED, "userId is required");
        }
    }

    private void validateMediaList(List<MediaUploadRequest> mediaList) {
        if (mediaList.size() > 1) {
            throw new AppException(ErrorCode.COMMENT_CREATE_FAILED, "A comment supports at most one media item");
        }
        for (MediaUploadRequest media : mediaList) {
            if (media == null
                    || media.getSecureUrl() == null || media.getSecureUrl().isBlank()
                    || media.getPublicId() == null || media.getPublicId().isBlank()) {
                throw new AppException(ErrorCode.COMMENT_CREATE_FAILED, "Comment media identifiers are required");
            }
        }
    }

    private void validateContent(String content) {
        if (content == null || content.trim().isEmpty()) {
            throw new AppException(ErrorCode.COMMENT_CONTENT_INVALID, "Comment content is empty");
        }
        String trimmed = content.trim();
        if (trimmed.length() < MIN_CONTENT_LENGTH || trimmed.length() > MAX_CONTENT_LENGTH) {
            throw new AppException(ErrorCode.COMMENT_CONTENT_INVALID, "Comment content length is invalid");
        }
        String normalized = trimmed.toLowerCase(Locale.ROOT);
        for (String banned : BANNED_WORDS) {
            if (normalized.contains(banned)) {
                throw new AppException(ErrorCode.COMMENT_CONTENT_INVALID, "Comment content contains prohibited words");
            }
        }
    }

    private AppException wrapCreateError(CommentCreateRequest request, Throwable error) {
        if (error instanceof AppException) {
            return (AppException) error;
        }
        log.error("|CommentWriteService|wrapCreateError|postId={}|error={}", request.getPostId(), error.getMessage());
        return new AppException(
                ErrorCode.COMMENT_CREATE_FAILED,
                String.format("Create comment failed for postId=%s", request.getPostId()),
                error
        );
    }

    private Mono<Void> sendImmediateCommentSuccess(Comment comment) {
        return sendCommentSuccessSse(comment);
    }

    private Mono<Void> sendCommentSuccessSse(Comment comment) {
        JsonObject payload = new JsonObject();
        payload.addProperty("commentId", comment.getId());
        payload.addProperty("userId", comment.getUserId());
        payload.addProperty("postId", comment.getPostId());
        payload.addProperty("content", comment.getContent());
        payload.addProperty("mediaUrl", comment.getMediaUrl());
        payload.addProperty("parentId", comment.getParentId());
        payload.addProperty("result", "SUCCESSED");
        payload.addProperty("message", "Comment approved");
        return userSsePublisher.sendToUser(
                comment.getUserId(),
                "comment_success_event",
                payload.toString());
    }

    private Mono<Void> sendTextCommentFailureSse(Comment comment, String message) {
        if (comment.getUserId() == null || comment.getUserId().isBlank()) {
            return Mono.empty();
        }
        JsonObject payload = new JsonObject();
        payload.addProperty("commentId", comment.getId());
        payload.addProperty("userId", comment.getUserId());
        payload.addProperty("postId", comment.getPostId());
        payload.addProperty("parentId", comment.getParentId());
        payload.addProperty("result", "FAILED");
        payload.addProperty("message", message);
        return userSsePublisher.sendToUser(
                comment.getUserId(),
                "comment_failed_event",
                payload.toString());
    }


}
