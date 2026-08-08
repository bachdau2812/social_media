package com.dauducbach.clone.modules.post.service.comment;

import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.post.service.post.MediaModerationProvider;
import com.dauducbach.clone.modules.post.service.post.PostSseService;
import com.dauducbach.clone.modules.media.service.MediaAssetCleanupService;
import com.dauducbach.clone.modules.media.service.MediaService;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.post.repositoty.CommentRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CommentMediaModerationOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(CommentMediaModerationOrchestrator.class);
    private static final String WAIT_UPLOAD_COMMENT_PREFIX = "wait_for_upload_comment:";
    private static final String COMMENT_SCAN_CLAIM_PREFIX = "comment_media_scan_claim:";
    private static final String POST_COMMENT_COUNT_PREFIX = "post_comment_count:";
    private static final String REPLY_COUNT_PREFIX = "reply_count:";
    private static final Duration PROCESSING_CLAIM_TTL = Duration.ofMinutes(5);
    private static final Duration COMPLETED_CLAIM_TTL = Duration.ofDays(2);

    private final CommentRepository commentRepository;
    private final MediaService mediaService;
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final PostSseService postSseService;
    private final KafkaSender<String, String> kafkaSender;
    private final MediaCompatibilityFacade cloudinaryMediaService;
    private final MediaModerationProvider moderationProvider;
    private final MediaAssetCleanupService cleanupService;

    public Mono<Void> process(
            String commentId,
            String postId,
            List<MediaUploadRequest> mediaList
    ) {
        if (commentId == null || commentId.isBlank()
                || postId == null || postId.isBlank()
                || mediaList == null || mediaList.isEmpty()) {
            return Mono.empty();
        }

        String claimKey = claimKey(commentId);
        return redisTemplate.opsForValue()
                .setIfAbsent(claimKey, "PROCESSING", PROCESSING_CLAIM_TTL)
                .defaultIfEmpty(false)
                .flatMap(claimed -> Boolean.TRUE.equals(claimed)
                        ? processClaimed(commentId, postId, mediaList)
                                .then(markCompleted(claimKey))
                                .onErrorResume(error -> redisTemplate.delete(claimKey)
                                        .then(Mono.error(error)))
                        : Mono.empty())
                .doOnError(error -> log.error(
                        "|CommentMediaModerationOrchestrator|process|commentId={}|error={}",
                        commentId, error.getMessage()));
    }

    private Mono<Void> processClaimed(
            String commentId,
            String postId,
            List<MediaUploadRequest> mediaList
    ) {
        return scanMediaList(mediaList)
                .flatMap(rejected -> rejected
                        ? handleCommentScanFailed(commentId, postId, mediaList)
                        : handleCommentScanSuccessWithMedia(commentId, postId, mediaList));
    }

    private Mono<Boolean> scanMediaList(List<MediaUploadRequest> mediaList) {
        return Flux.fromIterable(mediaList)
                .concatMap(item -> moderationProvider.scan(item.getSecureUrl(), item.getPublicId()))
                .any(decision -> decision == MediaModerationProvider.Decision.REJECTED);
    }

    private Mono<Void> handleCommentScanFailed(
            String commentId,
            String postId,
            List<MediaUploadRequest> mediaList
    ) {
        String waitKey = WAIT_UPLOAD_COMMENT_PREFIX + commentId;
        List<String> publicIds = mediaList.stream()
                .map(MediaUploadRequest::getPublicId)
                .toList();
        return commentRepository.findById(commentId)
                .flatMap(comment -> commentRepository.deleteById(commentId)
                        .then(decrementCounts(postId, comment.getParentId()))
                        .then(sendCommentFailureSse(comment)))
                .switchIfEmpty(commentRepository.deleteById(commentId).then())
                .then(cleanupService.deleteAll(publicIds))
                .then(redisTemplate.opsForValue().delete(waitKey).then());
    }

    private Mono<Void> handleCommentScanSuccessWithMedia(
            String commentId,
            String postId,
            List<MediaUploadRequest> mediaList
    ) {
        String waitKey = WAIT_UPLOAD_COMMENT_PREFIX + commentId;
        return commentRepository.findById(commentId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.COMMENT_NOT_FOUND,
                        String.format("Comment not found for commentId=%s", commentId))))
                .flatMap(comment -> fetchAndPersistCommentMedia(comment, postId, mediaList))
                .flatMap(saved -> redisTemplate.opsForValue().get(waitKey)
                        .defaultIfEmpty("")
                        .flatMap(userId -> sendCommentSuccessSse(userId, saved)
                                .then(sendCommentSuccessEvent(saved))))
                .then(redisTemplate.opsForValue().delete(waitKey).then());
    }

    private Mono<Comment> fetchAndPersistCommentMedia(
            Comment comment,
            String postId,
            List<MediaUploadRequest> mediaList
    ) {
        List<String> publicIds = mediaList.stream()
                .map(MediaUploadRequest::getPublicId)
                .filter(publicId -> publicId != null && !publicId.isBlank())
                .toList();

        return cloudinaryMediaService.fetchMediaList(publicIds, comment.getId(), OwnerType.COMMENT)
                .collectList()
                .flatMap(mediaEntities -> {
                    if (mediaEntities.size() != 1
                            || !moderationProvider.isAllowedAsset(mediaEntities.get(0))) {
                        return handleCommentScanFailed(comment.getId(), postId, mediaList)
                                .then(Mono.empty());
                    }

                    MediaUploadRequest uploaded = mediaList.get(0);
                    comment.setMediaUrl(uploaded.getSecureUrl());
                    comment.setCommentType("MEDIA");
                    return commentRepository.save(comment)
                            .flatMap(saved -> Flux.fromIterable(mediaEntities)
                                    .concatMap(media -> mediaService.registerFetchedMedia(media, comment.getId(), OwnerType.COMMENT))
                                    .then(Mono.just(saved)));
                });
    }

    private Mono<Void> markCompleted(String claimKey) {
        return redisTemplate.opsForValue()
                .set(claimKey, "COMPLETED", COMPLETED_CLAIM_TTL)
                .then();
    }

    private Mono<Void> sendCommentSuccessSse(String userId, Comment comment) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        JsonObject payload = new JsonObject();
        payload.addProperty("commentId", comment.getId());
        payload.addProperty("postId", comment.getPostId());
        payload.addProperty("result", "SUCCESSED");
        payload.addProperty("message", "Comment approved");
        return postSseService.sendToUser(userId, "comment_success_event", payload.toString());
    }

    private Mono<Void> sendCommentFailureSse(Comment comment) {
        JsonObject payload = new JsonObject();
        payload.addProperty("commentId", comment.getId());
        payload.addProperty("postId", comment.getPostId());
        payload.addProperty("result", "FAILED");
        payload.addProperty("message", "Comment rejected due to invalid media");
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
                new ProducerRecord<>(
                        "comment_success_event",
                        comment.getId(),
                        payload.toString()),
                "comment_success_event");
        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error(
                        "|CommentMediaModerationOrchestrator|sendCommentSuccessEvent|commentId={}|error={}",
                        comment.getId(), error.getMessage()))
                .then();
    }

    private Mono<Void> decrementCounts(String postId, String parentId) {
        List<Mono<Long>> operations = new ArrayList<>();
        if (postId != null && !postId.isBlank()) {
            operations.add(redisTemplate.opsForValue()
                    .decrement(POST_COMMENT_COUNT_PREFIX + postId));
        }
        if (parentId != null && !parentId.isBlank()) {
            operations.add(redisTemplate.opsForValue()
                    .decrement(REPLY_COUNT_PREFIX + parentId));
        }
        return operations.isEmpty() ? Mono.empty() : Mono.when(operations).then();
    }

    private String claimKey(String commentId) {
        return COMMENT_SCAN_CLAIM_PREFIX + commentId;
    }
}
