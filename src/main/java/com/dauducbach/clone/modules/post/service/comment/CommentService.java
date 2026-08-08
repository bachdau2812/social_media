package com.dauducbach.clone.modules.post.service.comment;

import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.post.service.post.PostSseService;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.dto.request.CommentCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.CommentUpdateRequest;
import com.dauducbach.clone.modules.post.dto.response.CommentCreateResponse;
import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.entity.Like;
import com.dauducbach.clone.modules.post.repositoty.CommentRepository;
import com.google.gson.JsonObject;
import com.dauducbach.clone.utils.GsonUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class CommentService {
    private static final Logger log = LoggerFactory.getLogger(CommentService.class);
    private static final String POST_COMMENT_COUNT_PREFIX = "post_comment_count:";
    private static final String POST_COMMENT_COUNT_LOCK_PREFIX = "post_comment_count_lock:";
    private static final String WAIT_UPLOAD_PREFIX = "wait_for_upload_comment:";
    private static final Duration WAIT_UPLOAD_TTL = Duration.ofHours(1);
    private static final Duration COUNT_CACHE_TTL = Duration.ofHours(24);
    private static final Duration COUNT_LOCK_TTL = Duration.ofSeconds(5);
    private static final Duration COUNT_LOCK_RETRY_DELAY = Duration.ofMillis(50);
    private static final int COUNT_LOCK_RETRY_ATTEMPTS = 5;
    private static final int DEFAULT_PAGE_SIZE = 10;
    private static final int MAX_PAGE_SIZE = 100;
    private static final int MIN_CONTENT_LENGTH = 1;
    private static final int MAX_CONTENT_LENGTH = 2000;
    private static final Set<String> BANNED_WORDS = Set.of("badword1", "badword2");

    CommentRepository commentRepository;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    KafkaSender<String, String> kafkaSender;
    PostSseService postSseService;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    MediaCompatibilityFacade cloudinaryMediaService;

    public Mono<CommentCreateResponse> createComment(CommentCreateRequest request) {
        log.info("|CommentService|createComment|start|postId={}|userId={}", request.getPostId(), request.getUserId());

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
                .mediaUrl(hasMedia ? mediaList.get(0).getSecureUrl() : null)
                .timestamp(Instant.now())
                .build();
        log.info("|CommentService|createComment|comment={}", comment);

        Mono<Void> waitKeyWrite = hasMedia
                ? reactiveRedisStringTemplate.opsForValue().set(WAIT_UPLOAD_PREFIX + commentId, request.getUserId(), WAIT_UPLOAD_TTL).then()
                : Mono.empty();

        Mono<Void> sendScanEvent = hasMedia
                ? sendCheckCommentMediaEvent(comment, mediaList)
                : Mono.empty();

        return validateParent(request)
                .then(ensurePostCommentCountCache(request.getPostId()))
                .then(r2dbcEntityTemplate.insert(Comment.class).using(comment))
                .doOnSuccess(comment1 -> log.info("|CommentService|createComment|insert_success={}", comment1.getId()))
                .doOnError(throwable -> log.error("|CommentService|createComment|insert_error|postId={}|userId={}|error={}",
                        request.getPostId(), request.getUserId(), throwable.getMessage(), throwable))
                .flatMap(saved -> {
                    Mono<Void> postSaveAction = hasMedia
                            ? waitKeyWrite.then(sendScanEvent)
                            : sendImmediateCommentSuccess(saved);
                    return updatePostCommentCountCache(saved.getPostId(), 1)
                            .then(postSaveAction)
                            .thenReturn(CommentCreateResponse.builder()
                            .commentId(saved.getId())
                            .message(hasMedia ? "Dang doi xu ly va duyet media" : "Comment created")
                            .build());
                })
                .doOnSuccess(response -> log.info("|CommentService|createComment|success|commentId={}", response.getCommentId()))
                .onErrorResume(error -> {
                    log.error("|CommentService|createComment|failed|postId={}|userId={}|error={}",
                            request.getPostId(), request.getUserId(), error.getMessage());
                    Mono<Void> failureNotification = hasMedia
                            ? Mono.empty()
                            : sendTextCommentFailureSse(comment, "Create comment failed");
                    return failureNotification.then(Mono.error(wrapCreateError(request, error)));
                });
    }

    public Mono<Comment> updateComment(CommentUpdateRequest request) {
        log.info("|CommentService|updateComment|start|commentId={}|userId={}", request.getCommentId(), request.getUserId());

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
                .doOnSuccess(updated -> log.info("|CommentService|updateComment|success|commentId={}", updated.getId()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.COMMENT_UPDATE_FAILED,
                                String.format("Update comment failed for commentId=%s", request.getCommentId()),
                                error
                        ));
    }

    public Mono<Void> deleteComment(String commentId, String userId) {
        log.info("|CommentService|deleteComment|start|commentId={}|userId={}", commentId, userId);

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
                    return ensurePostCommentCountCache(existing.getPostId())
                            .then(commentRepository.deleteById(commentId))
                            .then(updatePostCommentCountCache(existing.getPostId(), -1));
                })
                .doOnSuccess(unused -> log.info("|CommentService|deleteComment|success|commentId={}|userId={}", commentId, userId))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.COMMENT_DELETE_FAILED,
                                String.format("Delete comment failed for commentId=%s", commentId),
                                error
                        ));
    }

    public Flux<Comment> getRootComments(String postId, int page, int size) {
        return getRootComments(postId, null, page, size);
    }

    public Flux<Comment> getRootComments(String postId, String viewerId, int page, int size) {
        validatePostId(postId);
        int limit = validatePageSize(size);
        int offset = normalizePage(page) * limit;

        log.info("|CommentService|getRootComments|postId={}|viewerId={}|page={}|size={}", postId, viewerId, page, size);
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

        log.info("|CommentService|getChildComments|parentId={}|viewerId={}|page={}|size={}", parentId, viewerId, page, size);
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
        log.info("|CommentService|getCommentById|commentId={}", commentId);
        return commentRepository.findById(commentId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.COMMENT_NOT_FOUND,
                        String.format("Comment not found for commentId=%s", commentId)
                )))
                .map(this::transformCommentMedia)
                .doOnSuccess(comment -> log.info("|CommentService|getCommentById|success|commentId={}", commentId))
                .onErrorMap(error -> wrapFetchCommentError(commentId, error))
                .doOnError(error -> log.error("|CommentService|getCommentById|failed|commentId={}|error={}", commentId, error.getMessage()));
    }

    public Mono<PageResponse<String>> getCommentedPostIdsByUserId(String userId, int page, int size) {
        validateUserId(userId);
        int pageNumber = normalizePage(page);
        int pageSize = validatePageSize(size);
        int offset = pageNumber * pageSize;

        log.info("|CommentService|getCommentedPostIdsByUserId|userId={}|page={}|size={}", userId, pageNumber, pageSize);

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

        log.info("|CommentService|getDistinctCommenterUserIdsByPostId|postId={}", postId);
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

        log.info("|CommentService|getCommentsByUserId|userId={}|page={}|size={}", userId, pageNumber, pageSize);

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

        log.info("|CommentService|countCommentsByPostId|postId={}", postId);
        return getPostCommentCountFromCache(postId)
                .onErrorMap(error -> new AppException(
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Count comments failed for postId=%s", postId),
                        error
                ));
    }

    public Mono<Long> countRepliesByParentId(String parentId) {
        validateCommentId(parentId);

        log.info("|CommentService|countRepliesByParentId|parentId={}", parentId);
        return commentRepository.countByParentId(parentId)
                .onErrorMap(error -> new AppException(
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Count replies failed for parentId=%s", parentId),
                        error
                ));
    }

    private Mono<Long> getPostCommentCountFromCache(String postId) {
        String cacheKey = postCommentCountKey(postId);
        return readLongCache(cacheKey)
                .switchIfEmpty(Mono.defer(() -> withCountLock(POST_COMMENT_COUNT_LOCK_PREFIX + postId,
                        () -> readLongCache(cacheKey)
                                .switchIfEmpty(Mono.defer(() -> loadAndSetCountCache(cacheKey, () -> commentRepository.countByPostId(postId)))),
                        ErrorCode.COMMENT_FETCH_FAILED,
                        String.format("Load post comment count cache failed for postId=%s", postId)
                )));
    }

    private Mono<Void> ensurePostCommentCountCache(String postId) {
        String cacheKey = postCommentCountKey(postId);
        return withCountLock(POST_COMMENT_COUNT_LOCK_PREFIX + postId,
                () -> readLongCache(cacheKey)
                        .switchIfEmpty(Mono.defer(() -> loadAndSetCountCache(cacheKey, () -> commentRepository.countByPostId(postId))))
                        .then(),
                ErrorCode.COMMENT_FETCH_FAILED,
                String.format("Ensure post comment count cache failed for postId=%s", postId)
        );
    }

    private Mono<Void> updatePostCommentCountCache(String postId, long delta) {
        String cacheKey = postCommentCountKey(postId);
        return withCountLock(POST_COMMENT_COUNT_LOCK_PREFIX + postId,
                () -> reactiveRedisStringTemplate.opsForValue().increment(cacheKey, delta)
                        .flatMap(value -> value < 0
                                ? reactiveRedisStringTemplate.opsForValue().set(cacheKey, "0", COUNT_CACHE_TTL).then()
                                : reactiveRedisStringTemplate.expire(cacheKey, COUNT_CACHE_TTL).then()),
                ErrorCode.COMMENT_FETCH_FAILED,
                String.format("Update post comment count cache failed for postId=%s", postId)
        );
    }

    private Mono<Long> readLongCache(String cacheKey) {
        return reactiveRedisStringTemplate.opsForValue()
                .get(cacheKey)
                .filter(value -> value != null && !value.isBlank())
                .map(Long::parseLong);
    }

    private Mono<Long> loadAndSetCountCache(String cacheKey, Supplier<Mono<Long>> dbCountSupplier) {
        return dbCountSupplier.get()
                .flatMap(count -> reactiveRedisStringTemplate.opsForValue()
                        .set(cacheKey, String.valueOf(count), COUNT_CACHE_TTL)
                        .thenReturn(count));
    }

    private <T> Mono<T> withCountLock(String lockKey, Supplier<Mono<T>> operation, ErrorCode errorCode, String detailMessage) {
        return withCountLock(lockKey, UUID.randomUUID().toString(), operation, COUNT_LOCK_RETRY_ATTEMPTS)
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(errorCode, detailMessage, error));
    }

    private <T> Mono<T> withCountLock(String lockKey, String lockToken, Supplier<Mono<T>> operation, int attemptsLeft) {
        return reactiveRedisStringTemplate.opsForValue()
                .setIfAbsent(lockKey, lockToken, COUNT_LOCK_TTL)
                .flatMap(acquired -> {
                    if (Boolean.TRUE.equals(acquired)) {
                        return operation.get()
                                .materialize()
                                .flatMap(signal -> releaseCountLock(lockKey, lockToken).thenReturn(signal))
                                .dematerialize();
                    }

                    if (attemptsLeft <= 0) {
                        return Mono.error(new AppException(ErrorCode.COMMENT_FETCH_FAILED, "Cannot acquire count cache lock"));
                    }

                    return Mono.delay(COUNT_LOCK_RETRY_DELAY)
                            .then(withCountLock(lockKey, lockToken, operation, attemptsLeft - 1));
                });
    }

    private Mono<Void> releaseCountLock(String lockKey, String lockToken) {
        return reactiveRedisStringTemplate.opsForValue()
                .get(lockKey)
                .flatMap(currentToken -> lockToken.equals(currentToken)
                        ? reactiveRedisStringTemplate.delete(lockKey).then()
                        : Mono.empty())
                .then();
    }

    private String postCommentCountKey(String postId) {
        return POST_COMMENT_COUNT_PREFIX + postId;
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

    private Comment transformCommentMedia(Comment comment) {
        if (comment.getMediaUrl() != null && !comment.getMediaUrl().isBlank()) {
            comment.setMediaUrl(cloudinaryMediaService.transformDeliveryUrl(
                    comment.getMediaUrl(),
                    MediaDisplayType.COMMENT
            ));
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
        log.error("|CommentService|wrapCreateError|postId={}|error={}", request.getPostId(), error.getMessage());
        return new AppException(
                ErrorCode.COMMENT_CREATE_FAILED,
                String.format("Create comment failed for postId=%s", request.getPostId()),
                error
        );
    }

    private Mono<Void> sendCheckCommentMediaEvent(Comment comment, List<MediaUploadRequest> mediaList) {
        JsonObject payload = new JsonObject();
        payload.addProperty("commentId", comment.getId());
        payload.addProperty("postId", comment.getPostId());
        payload.addProperty("userId", comment.getUserId());
        payload.add("media", GsonUtils.getGson().toJsonTree(mediaList));

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>("check_comment_media_event", comment.getId(), payload.toString()),
                "check_comment_media_event"
        );

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|CommentService|sendCheckCommentMediaEvent|commentId={}|error={}", comment.getId(), error.getMessage()))
                .then();
    }

    private Mono<Void> sendImmediateCommentSuccess(Comment comment) {
        return sendCommentSuccessSse(comment)
                .then(sendCommentSuccessEvent(comment).onErrorResume(error -> {
                    log.error("|CommentService|sendImmediateCommentSuccess|event publish failed|commentId={}|error={}",
                            comment.getId(), error.getMessage());
                    return Mono.empty();
                }));
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
        return postSseService.sendToUser(
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
        return postSseService.sendToUser(
                comment.getUserId(),
                "comment_failed_event",
                payload.toString());
    }

    private Mono<Void> sendCommentSuccessEvent(Comment comment) {
        JsonObject payload = new JsonObject();
        payload.addProperty("commentId", comment.getId());
        payload.addProperty("userId", comment.getUserId());
        payload.addProperty("postId", comment.getPostId());
        payload.addProperty("content", comment.getContent());
        payload.addProperty("mediaUrl", comment.getMediaUrl());
        payload.addProperty("parentId", comment.getParentId());

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>("comment_success_event", comment.getId(), payload.toString()),
                "comment_success_event"
        );

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|CommentService|sendCommentSuccessEvent|commentId={}|error={}", comment.getId(), error.getMessage()))
                .then();
    }
}


