package com.dauducbach.clone.modules.post.service;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.constant.OwnerType;
import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.entity.Media;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repositoty.CommentRepository;
import com.dauducbach.clone.modules.post.repositoty.MediaRepository;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.dauducbach.clone.utils.MediaScanUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class ImageScanWorker {
    private static final Logger log = LoggerFactory.getLogger(ImageScanWorker.class);
    private static final String WAIT_UPLOAD_POST_PREFIX = "wait_for_upload_post:";
    private static final String WAIT_UPLOAD_COMMENT_PREFIX = "wait_for_upload_comment:";
    private static final String POST_COMMENT_COUNT_PREFIX = "post_comment_count:";
    private static final String REPLY_COUNT_PREFIX = "reply_count:";

    PostDetailsRepository postDetailsRepository;
    CommentRepository commentRepository;
    MediaRepository mediaRepository;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    PostSseService postSseService;
    KafkaSender<String, String> kafkaSender;
    Cloudinary cloudinary;
    CloudinaryMediaService cloudinaryMediaService;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    MediaScanUtils mediaScanUtils;

    @KafkaListener(topics = "check_media_event", groupId = "post-service")
    public void handlePostScanEvent(@Payload String payload) {
        log.info("|ImageScanWorker|handlePostScanEvent|start|payload={}", payload);
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String postId = KafkaUtils.extractString(payloadJson, "postId");
        List<MediaUploadRequest> mediaList = extractMediaList(payloadJson);

        if (postId.isBlank() || mediaList.isEmpty()) {
            log.warn("|ImageScanWorker|handlePostScanEvent|missing data|postId={}|mediaCount={}", postId, mediaList.size());
            return;
        }

        log.info("|ImageScanWorker|handlePostScanEvent|processing|postId={}|mediaCount={}", postId, mediaList.size());

        scanMediaList(mediaList)
                .flatMap(isNsfw -> isNsfw
                        ? handlePostScanFailed(postId, mediaList)
                        : handlePostScanSuccessWithMedia(postId, mediaList))
                .doOnSuccess(v -> log.info("|ImageScanWorker|handlePostScanEvent|completed|postId={}", postId))
                .doOnError(error -> log.error("|ImageScanWorker|handlePostScanEvent|failed|postId={}|error={}",
                        postId, error.getMessage()))
                .subscribe();
    }

    @KafkaListener(topics = "check_comment_media_event", groupId = "post-service")
    public void handleCommentScanEvent(@Payload String payload) {
        log.info("|ImageScanWorker|handleCommentScanEvent|start|payload={}", payload);
        JsonObject payloadJson = GsonUtils.fromString(payload);
        String commentId = KafkaUtils.extractString(payloadJson, "commentId");
        String postId = KafkaUtils.extractString(payloadJson, "postId");
        List<MediaUploadRequest> mediaList = extractMediaList(payloadJson);

        if (commentId.isBlank() || postId.isBlank() || mediaList.isEmpty()) {
            log.warn("|ImageScanWorker|handleCommentScanEvent|missing data|commentId={}|postId={}|mediaCount={}",
                    commentId, postId, mediaList.size());
            return;
        }

        log.info("|ImageScanWorker|handleCommentScanEvent|processing|commentId={}|postId={}|mediaCount={}",
                commentId, postId, mediaList.size());

        scanMediaList(mediaList)
                .flatMap(isNsfw -> isNsfw
                        ? handleCommentScanFailed(commentId, postId, mediaList)
                        : handleCommentScanSuccessWithMedia(commentId, postId, mediaList))
                .doOnSuccess(v -> log.info("|ImageScanWorker|handleCommentScanEvent|completed|commentId={}", commentId))
                .doOnError(error -> log.error("|ImageScanWorker|handleCommentScanEvent|failed|commentId={}|error={}",
                        commentId, error.getMessage()))
                .subscribe();
    }

    private Mono<Boolean> scanMediaList(List<MediaUploadRequest> mediaList) {
        return Flux.fromIterable(mediaList)
                .concatMap(item -> mediaScanUtils.scanMedia(item.getSecureUrl(), item.getPublicId()))
                .any(MediaScanUtils.ScanResult::isNsfw)
                .onErrorResume(error -> {
                    log.error("|ImageScanWorker|scanMediaList|failed|error={}", error.getMessage());
                    return Mono.just(true);
                });
    }

    private Mono<Void> handlePostScanFailed(String postId, List<MediaUploadRequest> mediaList) {
        String waitKey = WAIT_UPLOAD_POST_PREFIX + postId;
        return reactiveRedisStringTemplate.opsForValue().get(waitKey)
                .defaultIfEmpty("")
                .flatMap(userId -> postDetailsRepository.deleteById(postId)
                        .then(deleteCloudinaryMedia(mediaList))
                        .then(sendPostFailureSse(userId, postId))
                        .then(reactiveRedisStringTemplate.opsForValue().delete(waitKey).then()))
                .doOnError(error -> log.error("|ImageScanWorker|handlePostScanFailed|postId={}|error={}", postId, error.getMessage()))
                .onErrorResume(error -> Mono.empty());
    }

    /**
     * Handle post scan success and store media metadata from Cloudinary.
     * This method is called after media scan passes validation.
     * Fetches detailed media information from Cloudinary API and stores in database.
     *
     * @param postId the post ID
     * @param mediaList list of media with publicId from Cloudinary
     * @return Mono indicating completion
     */
    private Mono<Void> handlePostScanSuccessWithMedia(String postId, List<MediaUploadRequest> mediaList) {
        String waitKey = WAIT_UPLOAD_POST_PREFIX + postId;
        return postDetailsRepository.findById(postId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.POST_NOT_FOUND,
                        String.format("Post not found for postId=%s", postId)
                )))
                .flatMap(post -> {
                    log.info("|ImageScanWorker|handlePostScanSuccessWithMedia|updating post status|postId={}|status=APPROVED", postId);

                    post.setValidateStatus("APPROVED");
                    post.setUpdatedAt(Instant.now());
                    return postDetailsRepository.save(post);
                })
                .flatMap(savedPost -> {
                    // Fetch and save media metadata from Cloudinary
                    if (mediaList != null && !mediaList.isEmpty()) {
                        List<String> publicIds = mediaList.stream()
                                .map(MediaUploadRequest::getPublicId)
                                .filter(pid -> pid != null && !pid.isBlank())
                                .toList();

                        if (!publicIds.isEmpty()) {
                            log.info("|ImageScanWorker|handlePostScanSuccessWithMedia|fetching media metadata|postId={}|count={}",
                                    postId, publicIds.size());

                            return cloudinaryMediaService.fetchMediaList(publicIds, savedPost.getPostId(), OwnerType.POST)
                                    .flatMap(media -> {
                                        log.info("|ImageScanWorker|handlePostScanSuccessWithMedia|saving media metadata|postId={}|assetId={}",
                                                postId, media.getAssetId());
                                        return r2dbcEntityTemplate.insert(Media.class).using(media)
                                                .doOnSuccess(saved -> log.info("|ImageScanWorker|handlePostScanSuccessWithMedia|media saved|postId={}|assetId={}",
                                                        postId, saved.getAssetId()))
                                                .onErrorResume(error -> {
                                                    log.error("|ImageScanWorker|handlePostScanSuccessWithMedia|media save failed|assetId={}|error={}",
                                                            media.getAssetId(), error.getMessage());
                                                    return Mono.empty();
                                                });
                                    })
                                    .then(Mono.just(savedPost))
                                    .switchIfEmpty(Mono.just(savedPost));
                        }
                    }
                    return Mono.just(savedPost);
                })
                .flatMap(post -> reactiveRedisStringTemplate.opsForValue().get(waitKey)
                        .defaultIfEmpty("")
                        .flatMap(userId -> sendPostSuccessSse(userId, post)
                                .then(sendPostUploadEvent(post))
                                .then()))
                .then(reactiveRedisStringTemplate.opsForValue().delete(waitKey).then())
                .doOnSuccess(v -> log.info("|ImageScanWorker|handlePostScanSuccessWithMedia|completed|postId={}", postId))
                .doOnError(error -> log.error("|ImageScanWorker|handlePostScanSuccessWithMedia|failed|postId={}|error={}", postId, error.getMessage()))
                .onErrorResume(error -> Mono.empty());
    }

    private Mono<Void> handleCommentScanFailed(String commentId, String postId, List<MediaUploadRequest> mediaList) {
        String waitKey = WAIT_UPLOAD_COMMENT_PREFIX + commentId;
        return commentRepository.findById(commentId)
                .flatMap(comment -> commentRepository.deleteById(commentId)
                        .then(decrementCounts(postId, comment.getParentId()))
                        .then(sendCommentFailureSse(comment))
                )
                .switchIfEmpty(commentRepository.deleteById(commentId).then())
                .then(deleteCloudinaryMedia(mediaList))
                .then(reactiveRedisStringTemplate.opsForValue().delete(waitKey).then())
                .doOnError(error -> log.error("|ImageScanWorker|handleCommentScanFailed|commentId={}|error={}", commentId, error.getMessage()))
                .onErrorResume(error -> Mono.empty());
    }

    private Mono<Void> handleCommentScanSuccess(String commentId, String postId, List<MediaUploadRequest> mediaList) {
        String waitKey = WAIT_UPLOAD_COMMENT_PREFIX + commentId;
        return commentRepository.findById(commentId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.COMMENT_NOT_FOUND,
                        String.format("Comment not found for commentId=%s", commentId)
                )))
                .flatMap(comment -> {
                    log.info("|ImageScanWorker|handleCommentScanSuccess|updating comment|commentId={}|postId={}", commentId, postId);

                    MediaUploadRequest first = mediaList.get(0);
                    if (first.getSecureUrl() != null && !first.getSecureUrl().isBlank()) {
                        comment.setMediaUrl(first.getSecureUrl());
                        comment.setCommentType("MEDIA");
                        log.info("|ImageScanWorker|handleCommentScanSuccess|comment set as MEDIA|commentId={}", commentId);
                    } else {
                        comment.setCommentType("TEXT");
                        log.info("|ImageScanWorker|handleCommentScanSuccess|comment set as TEXT|commentId={}", commentId);
                    }
                    return commentRepository.save(comment);
                })
                .flatMap(saved -> reactiveRedisStringTemplate.opsForValue().get(waitKey)
                        .defaultIfEmpty("")
                        .flatMap(userId -> sendCommentSuccessSse(userId, saved)
                                .then(sendCommentSuccessEvent(saved))
                                .then()))
                .then(reactiveRedisStringTemplate.opsForValue().delete(waitKey).then())
                .doOnSuccess(v -> log.info("|ImageScanWorker|handleCommentScanSuccess|completed|commentId={}", commentId))
                .doOnError(error -> log.error("|ImageScanWorker|handleCommentScanSuccess|failed|commentId={}|error={}",
                        commentId, error.getMessage()))
                .onErrorResume(error -> Mono.empty());
    }

    /**
     * Handle comment scan success and store media metadata from Cloudinary.
     * This method is called after media scan passes validation.
     * Fetches detailed media information from Cloudinary API and stores in database.
     *
     * @param commentId the comment ID
     * @param postId the post ID
     * @param mediaList list of media with publicId from Cloudinary
     * @return Mono indicating completion
     */
    private Mono<Void> handleCommentScanSuccessWithMedia(String commentId, String postId, List<MediaUploadRequest> mediaList) {
        String waitKey = WAIT_UPLOAD_COMMENT_PREFIX + commentId;
        return commentRepository.findById(commentId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.COMMENT_NOT_FOUND,
                        String.format("Comment not found for commentId=%s", commentId)
                )))
                .flatMap(comment -> {
                    log.info("|ImageScanWorker|handleCommentScanSuccessWithMedia|updating comment|commentId={}|postId={}", commentId, postId);

                    MediaUploadRequest first = mediaList.get(0);
                    if (first.getSecureUrl() != null && !first.getSecureUrl().isBlank()) {
                        comment.setMediaUrl(first.getSecureUrl());
                        comment.setCommentType("MEDIA");
                        log.info("|ImageScanWorker|handleCommentScanSuccessWithMedia|comment set as MEDIA|commentId={}", commentId);
                    } else {
                        comment.setCommentType("TEXT");
                        log.info("|ImageScanWorker|handleCommentScanSuccessWithMedia|comment set as TEXT|commentId={}", commentId);
                    }
                    return commentRepository.save(comment);
                })
                .flatMap(savedComment -> {
                    // Fetch and save media metadata from Cloudinary
                    if (mediaList != null && !mediaList.isEmpty()) {
                        List<String> publicIds = mediaList.stream()
                                .map(MediaUploadRequest::getPublicId)
                                .filter(pid -> pid != null && !pid.isBlank())
                                .toList();

                        if (!publicIds.isEmpty()) {
                            log.info("|ImageScanWorker|handleCommentScanSuccessWithMedia|fetching media metadata|commentId={}|count={}",
                                    commentId, publicIds.size());

                            return cloudinaryMediaService.fetchMediaList(publicIds, savedComment.getId(), OwnerType.COMMENT)
                                    .flatMap(media -> {
                                        log.info("|ImageScanWorker|handleCommentScanSuccessWithMedia|saving media metadata|commentId={}|assetId={}",
                                                commentId, media.getAssetId());
                                        return r2dbcEntityTemplate.insert(Media.class).using(media)
                                                .doOnSuccess(saved -> log.info("|ImageScanWorker|handleCommentScanSuccessWithMedia|media saved|commentId={}|assetId={}",
                                                        commentId, saved.getAssetId()))
                                                .onErrorResume(error -> {
                                                    log.error("|ImageScanWorker|handleCommentScanSuccessWithMedia|media save failed|assetId={}|error={}",
                                                            media.getAssetId(), error.getMessage());
                                                    return Mono.empty();
                                                });
                                    })
                                    .then(Mono.just(savedComment))
                                    .switchIfEmpty(Mono.just(savedComment));
                        }
                    }
                    return Mono.just(savedComment);
                })
                .flatMap(saved -> reactiveRedisStringTemplate.opsForValue().get(waitKey)
                        .defaultIfEmpty("")
                        .flatMap(userId -> sendCommentSuccessSse(userId, saved)
                                .then(sendCommentSuccessEvent(saved))
                                .then()))
                .then(reactiveRedisStringTemplate.opsForValue().delete(waitKey).then())
                .doOnSuccess(v -> log.info("|ImageScanWorker|handleCommentScanSuccessWithMedia|completed|commentId={}", commentId))
                .doOnError(error -> log.error("|ImageScanWorker|handleCommentScanSuccessWithMedia|failed|commentId={}|error={}",
                        commentId, error.getMessage()))
                .onErrorResume(error -> Mono.empty());
    }

    private Mono<Void> sendPostSuccessSse(String userId, PostDetails post) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        JsonObject response = new JsonObject();
        response.addProperty("postId", post.getPostId());
        response.addProperty("result", "SUCCESSED");
        response.addProperty("message", "Post approved");
        postSseService.sendToUser(userId, "post_upload", response.toString());
        return Mono.empty();
    }

    private Mono<Void> sendPostFailureSse(String userId, String postId) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        JsonObject response = new JsonObject();
        response.addProperty("postId", postId);
        response.addProperty("result", "FAILED");
        response.addProperty("message", "Post rejected due to invalid media");
        postSseService.sendToUser(userId, "post_upload", response.toString());
        return Mono.empty();
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
        postSseService.sendToUser(userId, "comment_success_event", payload.toString());
        return Mono.empty();
    }

    private Mono<Void> sendCommentFailureSse(Comment comment) {
        JsonObject payload = new JsonObject();
        payload.addProperty("commentId", comment.getId());
        payload.addProperty("postId", comment.getPostId());
        payload.addProperty("result", "FAILED");
        payload.addProperty("message", "Comment rejected due to invalid media");
        postSseService.sendToUser(comment.getUserId(), "comment_failed_event", payload.toString());
        return Mono.empty();
    }

    private Mono<Void> sendPostUploadEvent(PostDetails postDetails) {
        JsonObject payload = new JsonObject();
        payload.addProperty("post_id", postDetails.getPostId());
        payload.addProperty("userId", postDetails.getUserId());
        payload.addProperty("content", postDetails.getContent());
        payload.add("hashtag", GsonUtils.getGson().toJsonTree(postDetails.getHashtagList()));

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>("post_upload_event", postDetails.getPostId(), payload.toString()),
                "post_upload_event"
        );

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|ImageScanWorker|sendPostUploadEvent|postId={}|error={}", postDetails.getPostId(), error.getMessage()))
                .then();
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
                .doOnError(error -> log.error("|ImageScanWorker|sendCommentSuccessEvent|commentId={}|error={}", comment.getId(), error.getMessage()))
                .then();
    }

    private Mono<Void> deleteCloudinaryMedia(List<MediaUploadRequest> mediaList) {
        return Flux.fromIterable(mediaList)
                .filter(item -> item.getPublicId() != null && !item.getPublicId().isBlank())
                .flatMap(item -> Mono.fromCallable(() -> cloudinary.uploader().destroy(item.getPublicId(), ObjectUtils.emptyMap()))
                        .subscribeOn(Schedulers.boundedElastic())
                        .onErrorResume(error -> {
                            log.error("|ImageScanWorker|deleteCloudinaryMedia|publicId={}|error={}", item.getPublicId(), error.getMessage());
                            return Mono.empty();
                        }))
                .then();
    }

    private Mono<Void> decrementCounts(String postId, String parentId) {
        List<Mono<Long>> operations = new ArrayList<>();
        if (postId != null && !postId.isBlank()) {
            operations.add(reactiveRedisStringTemplate.opsForValue().decrement(POST_COMMENT_COUNT_PREFIX + postId));
        }
        if (parentId != null && !parentId.isBlank()) {
            operations.add(reactiveRedisStringTemplate.opsForValue().decrement(REPLY_COUNT_PREFIX + parentId));
        }
        return operations.isEmpty() ? Mono.empty() : Mono.when(operations).then();
    }

    private List<MediaUploadRequest> extractMediaList(JsonObject payloadJson) {
        List<MediaUploadRequest> mediaList = new ArrayList<>();
        if (payloadJson.has("media") && payloadJson.get("media").isJsonArray()) {
            JsonArray array = payloadJson.getAsJsonArray("media");
            for (JsonElement element : array) {
                if (!element.isJsonObject()) {
                    continue;
                }
                JsonObject item = element.getAsJsonObject();
                String secureUrl = KafkaUtils.extractString(item, "secureUrl");
                String publicId = KafkaUtils.extractString(item, "publicId");
                mediaList.add(MediaUploadRequest.builder()
                        .secureUrl(secureUrl)
                        .publicId(publicId)
                        .build());
            }
        }

        if (mediaList.isEmpty()) {
            String secureUrl = KafkaUtils.extractString(payloadJson, "secureUrl");
            String publicId = KafkaUtils.extractString(payloadJson, "publicId");
            if (!secureUrl.isBlank() || !publicId.isBlank()) {
                mediaList.add(MediaUploadRequest.builder()
                        .secureUrl(secureUrl)
                        .publicId(publicId)
                        .build());
            }
        }

        return mediaList;
    }
}
