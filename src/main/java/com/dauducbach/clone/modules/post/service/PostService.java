package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.constant.PostNotificationCacheKeys;
import com.dauducbach.clone.modules.post.dto.request.PostCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostUpdateRequest;
import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.modules.post.dto.response.PostCreateResponse;
import com.dauducbach.clone.modules.post.dto.response.PostNotificationMuteResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.RedisUtil;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jsoup.Jsoup;
import org.jsoup.safety.Safelist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class PostService {
    PostDetailsRepository postDetailsRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    KafkaSender<String, String> kafkaSender;
    PostSseService postSseService;

    private static final Logger log = LoggerFactory.getLogger(PostService.class);
    private static final String POST_CACHE_PREFIX = "post_details:";
    private static final String WAIT_UPLOAD_PREFIX = "wait_for_upload_post:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);
    private static final Duration WAIT_UPLOAD_TTL = Duration.ofHours(1);
    private static final Duration POST_NOTIFICATION_MUTE_TTL = Duration.ofDays(60);
    private static final long POST_NOTIFICATION_MUTE_DAYS = 60L;

    public Mono<PostCreateResponse> createPost(PostCreateRequest request) {
        log.info("|PostService|createPost|start|userId={}|contentLength={}",
                request.getUserId(),
                request.getContent() != null ? request.getContent().length() : 0);

        if (request.getUserId() == null || request.getUserId().isBlank()) {
            log.warn("|PostService|createPost|validation failed|userId is missing");
            return Mono.error(new AppException(
                    ErrorCode.POST_CREATE_FAILED,
                    "userId is required"
            ));
        }

        List<MediaUploadRequest> mediaList = request.getMediaList() == null ? List.of() : request.getMediaList();
        boolean hasMedia = !mediaList.isEmpty();
        log.info("|PostService|createPost|media resolved|count={}", mediaList.size());

        String postStatus = hasMedia ? "PENDING_SCAN" : "APPROVED";

        String postId = UUID.randomUUID().toString();
        String content = sanitizeAndValidateContent(request.getContent());
        List<String> hashtags = request.getHashtag();

        log.info("|PostService|createPost|creating post entity|postId={}|userId={}", postId, request.getUserId());

        PostDetails postDetails = PostDetails.builder()
                .postId(postId)
                .userId(request.getUserId())
                .content(content)
                .validateStatus(postStatus)
                .build();
        postDetails.setCreatedAt(Instant.now());
        postDetails.setUpdatedAt(Instant.now());
        postDetails.setHashtagList(hashtags);

        String waitKey = WAIT_UPLOAD_PREFIX + postId;

        Mono<PostDetails> insertPost = r2dbcEntityTemplate.insert(PostDetails.class)
                .using(postDetails)
                .publishOn(Schedulers.boundedElastic())
                .doOnSuccess(saved -> log.info("|PostService|createPost|post saved to database|postId={}|status={}", postId, postStatus))
                .onErrorMap(throwable -> {
                    log.error("|PostService|createPost|database insert failed|postId={}|error={}",
                            postId, throwable.getMessage());
                    return new AppException(
                            ErrorCode.POST_CREATE_FAILED,
                            String.format("Create post failed for userId=%s", request.getUserId()),
                            throwable
                    );
                });

        Mono<Void> postCreateAction = hasMedia
                ? insertPost
                .flatMap(saved -> reactiveRedisStringTemplate.opsForValue()
                        .set(waitKey, saved.getUserId(), WAIT_UPLOAD_TTL)
                        .then(sendCheckMediaEvent(postId, request.getUserId(), mediaList)))
                .doOnSuccess(v -> log.info("|PostService|createPost|scan event sent|postId={}|mediaCount={}",
                        postId, mediaList.size()))
                : insertPost
                .flatMap(saved -> sendPostSuccessSse(saved)
                        .then(sendPostUploadEvent(saved).onErrorResume(error -> {
                            log.error("|PostService|createPost|text post event publish failed|postId={}|error={}",
                                    postId, error.getMessage());
                            return Mono.empty();
                        })))
                .doOnSuccess(v -> log.info("|PostService|createPost|text post approved|postId={}", postId));

        return postCreateAction
                .thenReturn(PostCreateResponse.builder()
                        .postId(postId)
                        .message(hasMedia ? "Dang doi xu ly va duyet media" : "Post created")
                .build())
                .doOnError(error -> {
                    log.error("|PostService|createPost|failed|postId={}|userId={}|error={}", postId, request.getUserId(), error.getMessage());
                    if (!hasMedia) {
                        sendPostFailureSse(request.getUserId(), postId, "Create post failed");
                    }
                })
                .doOnSuccess(response -> log.info("|PostService|createPost|completed|postId={}", postId));
    }

    public Mono<PostDetails> updatePost(PostUpdateRequest request) {
        String postId = request.getPostId();
        log.info("|PostService|updatePost|start|postId={}|userId={}", postId, request.getUserId());

        return postDetailsRepository.findById(postId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.POST_NOT_FOUND,
                        String.format("Post not found for postId=%s", postId)
                )))
                .flatMap(existing -> {
                    log.info("|PostService|updatePost|post found|postId={}|currentContentLength={}",
                            postId, existing.getContent() != null ? existing.getContent().length() : 0);

                    if (request.getContent() != null && !request.getContent().isBlank()) {
                        String sanitizedContent = sanitizeAndValidateContent(request.getContent());
                        existing.setContent(sanitizedContent);
                        log.info("|PostService|updatePost|content updated|postId={}|newContentLength={}",
                                postId, sanitizedContent.length());
                    }
                    if (request.getHashtag() != null && !request.getHashtag().isEmpty()) {
                        existing.setHashtagList(request.getHashtag());
                        log.info("|PostService|updatePost|hashtags updated|postId={}|count={}",
                                postId, request.getHashtag().size());
                    }
                    existing.setUpdatedAt(Instant.now());
                    return postDetailsRepository.save(existing);
                })
                .doOnSuccess(updated -> log.info("|PostService|updatePost|saved to database|postId={}", postId))
                .onErrorMap(throwable -> {
                    log.error("|PostService|updatePost|operation failed|postId={}|error={}",
                            postId, throwable.getMessage());
                    return throwable instanceof AppException
                            ? throwable
                            : new AppException(
                                    ErrorCode.POST_UPDATE_FAILED,
                                    String.format("Update post failed for postId=%s", postId),
                                    throwable
                            );
                })
                .publishOn(Schedulers.boundedElastic())
                .doOnSuccess(updated -> {
                    String cacheKey = POST_CACHE_PREFIX + updated.getPostId();
                    reactiveRedisStringTemplate.opsForValue().get(cacheKey)
                            .flatMap(existingCache -> {
                                String cacheValue = RedisUtil.serialize(updated);
                                if (cacheValue == null) {
                                    return Mono.empty();
                                }
                                return reactiveRedisStringTemplate.opsForValue().set(cacheKey, cacheValue, CACHE_TTL);
                            })
                            .subscribe();

                    publishPostEvent("post_update_event", updated);
                    log.info("|PostService|updatePost|cache updated and event published|postId={}", postId);
                })
                .doOnSuccess(updated -> log.info("|PostService|updatePost|completed|postId={}", postId));
    }

    public Mono<PostDetails> getPostById(String postId) {
        String cacheKey = POST_CACHE_PREFIX + postId;

        return reactiveRedisStringTemplate.opsForValue().get(cacheKey)
                .onErrorResume(error -> {
                    log.warn("|PostService|getPostById|cache read failed, fallback to database|postId={}|error={}", postId, error.getMessage());
                    return Mono.empty();
                })
                .flatMap(cacheValue -> {
                    PostDetails cached = RedisUtil.deserialize(cacheValue, PostDetails.class);
                    if (cached != null) {
                        return Mono.just(cached);
                    }
                    return Mono.empty();
                })
                .switchIfEmpty(postDetailsRepository.findById(postId)
                        .switchIfEmpty(Mono.error(new AppException(
                                ErrorCode.POST_NOT_FOUND,
                                String.format("Post not found for postId=%s", postId)
                        )))
                        .onErrorMap(throwable -> throwable instanceof AppException
                                ? throwable
                                : new AppException(
                                        ErrorCode.POST_FETCH_FAILED,
                                        String.format("Fetch post failed for postId=%s", postId),
                                        throwable
                                ))
                        .doOnSuccess(post -> {
                            String cacheValue = RedisUtil.serialize(post);
                            if (cacheValue != null) {
                                reactiveRedisStringTemplate.opsForValue().set(cacheKey, cacheValue, CACHE_TTL).subscribe();
                            }
                        })
                );
    }

    public Mono<String> getPostOwnerIdByPostId(String postId) {
        if (postId == null || postId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.POST_FETCH_FAILED, "postId is required"));
        }

        return postDetailsRepository.findUserIdByPostId(postId)
                .filter(userId -> userId != null && !userId.isBlank())
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.POST_NOT_FOUND,
                        String.format("Post not found for postId=%s", postId)
                )))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.POST_FETCH_FAILED,
                                String.format("Fetch post owner failed for postId=%s", postId),
                                throwable
                        ));
    }

    public Mono<PostNotificationMuteResponse> mutePostNotifications(String postId, String userId) {
        validatePostNotificationMuteRequest(postId, userId);

        String cacheKey = PostNotificationCacheKeys.mutedPostNotification(postId, userId);
        return postDetailsRepository.existsById(postId)
                .flatMap(exists -> {
                    if (!Boolean.TRUE.equals(exists)) {
                        return Mono.error(new AppException(
                                ErrorCode.POST_NOT_FOUND,
                                String.format("Post not found for postId=%s", postId)
                        ));
                    }

                    return reactiveRedisStringTemplate.opsForValue()
                            .set(cacheKey, "true", POST_NOTIFICATION_MUTE_TTL)
                            .thenReturn(new PostNotificationMuteResponse(postId, userId, POST_NOTIFICATION_MUTE_DAYS));
                })
                .doOnSuccess(response -> log.info("|PostService|mutePostNotifications|postId={}|userId={}|mutedDays={}",
                        response.postId(), response.userId(), response.mutedDays()))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.POST_NOTIFICATION_MUTE_FAILED,
                                String.format("Mute post notification failed for postId=%s|userId=%s", postId, userId),
                                throwable
                        ));
    }

    public Flux<PostDetails> getPostsByUserId(String userId, int page, int size) {
        int limit = size <= 0 ? 10 : Math.min(size, 50);
        int offset = Math.max(page, 0) * limit;

        return postDetailsRepository.findByUserId(userId, limit, offset)
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.POST_LIST_FETCH_FAILED,
                        String.format("Fetch posts failed for userId=%s", userId),
                        throwable
                ));
    }

    public Mono<Void> deletePostById(String postId) {
        String cacheKey = POST_CACHE_PREFIX + postId;
        String waitKey = WAIT_UPLOAD_PREFIX + postId;

        return postDetailsRepository.findById(postId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.POST_NOT_FOUND,
                        String.format("Post not found for postId=%s", postId)
                )))
                .flatMap(existing -> postDetailsRepository.deleteById(postId)
                        .then(reactiveRedisStringTemplate.opsForValue().delete(cacheKey).then())
                        .then(reactiveRedisStringTemplate.opsForValue().delete(waitKey).then())
                )
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.POST_DELETE_FAILED,
                                String.format("Delete post failed for postId=%s", postId),
                                throwable
                        ));
    }

    public Mono<Void> deletePostsByUserId(String userId) {
        return postDetailsRepository.findAllByUserId(userId)
                .collectList()
                .flatMap(posts -> {
                    Mono<Void> cacheRemoval = Flux.fromIterable(posts)
                            .flatMap(post -> {
                                String cacheKey = POST_CACHE_PREFIX + post.getPostId();
                                String waitKey = WAIT_UPLOAD_PREFIX + post.getPostId();
                                return reactiveRedisStringTemplate.opsForValue().delete(cacheKey)
                                        .then(reactiveRedisStringTemplate.opsForValue().delete(waitKey))
                                        .then();
                            })
                            .then();

                    return cacheRemoval.then(postDetailsRepository.deleteByUserId(userId));
                })
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.POST_DELETE_FAILED,
                        String.format("Delete posts failed for userId=%s", userId),
                        throwable
                ));
    }

    private String sanitizeAndValidateContent(String content) {
        if (content == null || content.isBlank()) {
            throw new AppException(ErrorCode.POST_CONTENT_INVALID, "Post content is empty");
        }

        String sanitized = Jsoup.clean(content, Safelist.relaxed());
        if (sanitized.isBlank()) {
            throw new AppException(ErrorCode.POST_CONTENT_INVALID, "Post content is invalid after sanitization");
        }

        return sanitized;
    }

    private void validatePostNotificationMuteRequest(String postId, String userId) {
        if (postId == null || postId.isBlank()) {
            throw new AppException(ErrorCode.POST_NOTIFICATION_MUTE_FAILED, "postId is required");
        }
        if (userId == null || userId.isBlank()) {
            throw new AppException(ErrorCode.POST_NOTIFICATION_MUTE_FAILED, "userId is required");
        }
    }

    private void publishPostEvent(String topic, PostDetails postDetails) {
        JsonObject payload = new JsonObject();
        payload.addProperty("post_id", postDetails.getPostId());
        payload.addProperty("content", postDetails.getContent());
        payload.add("hashtag", com.dauducbach.clone.utils.GsonUtils.getGson().toJsonTree(postDetails.getHashtagList()));

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(topic, postDetails.getPostId(), payload.toString()),
                topic
        );

        kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|PostService|publishPostEvent|topic={}|error={}", topic, error.getMessage()))
                .subscribe();
    }

    private Mono<Void> sendPostUploadEvent(PostDetails postDetails) {
        JsonObject payload = buildPostEventPayload(postDetails);

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>("post_upload_event", postDetails.getPostId(), payload.toString()),
                "post_upload_event"
        );

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|PostService|sendPostUploadEvent|postId={}|error={}", postDetails.getPostId(), error.getMessage()))
                .then();
    }

    private JsonObject buildPostEventPayload(PostDetails postDetails) {
        JsonObject payload = new JsonObject();
        payload.addProperty("post_id", postDetails.getPostId());
        payload.addProperty("userId", postDetails.getUserId());
        payload.addProperty("content", postDetails.getContent());
        payload.add("hashtag", GsonUtils.getGson().toJsonTree(postDetails.getHashtagList()));
        return payload;
    }

    private Mono<Void> sendPostSuccessSse(PostDetails postDetails) {
        JsonObject payload = new JsonObject();
        payload.addProperty("postId", postDetails.getPostId());
        payload.addProperty("result", "SUCCESSED");
        payload.addProperty("message", "Post approved");
        postSseService.sendToUser(postDetails.getUserId(), "post_upload", payload.toString());
        return Mono.empty();
    }

    private void sendPostFailureSse(String userId, String postId, String message) {
        if (userId == null || userId.isBlank()) {
            return;
        }

        JsonObject payload = new JsonObject();
        payload.addProperty("postId", postId);
        payload.addProperty("result", "FAILED");
        payload.addProperty("message", message);
        postSseService.sendToUser(userId, "post_upload", payload.toString());
    }

    private Mono<Void> sendCheckMediaEvent(String postId, String userId, List<MediaUploadRequest> mediaList) {
        JsonObject payload = new JsonObject();
        payload.addProperty("postId", postId);
        payload.addProperty("userId", userId);
        payload.add("media", GsonUtils.getGson().toJsonTree(mediaList));

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>("check_media_event", postId, payload.toString()),
                "check_media_event"
        );

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|PostService|sendCheckMediaEvent|postId={}|error={}", postId, error.getMessage()))
                .then();
    }
}
