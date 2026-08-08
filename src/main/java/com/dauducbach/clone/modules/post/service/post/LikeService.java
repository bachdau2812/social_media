package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.event.LikeEventPayload;
import com.dauducbach.clone.modules.post.dto.request.LikeRequest;
import com.dauducbach.clone.modules.post.dto.response.LikeToggleResponse;
import com.dauducbach.clone.modules.post.entity.Like;
import com.dauducbach.clone.modules.post.repositoty.CommentRepository;
import com.dauducbach.clone.modules.post.repositoty.LikeRepository;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)

public class LikeService {
    private static final Logger log = LoggerFactory.getLogger(LikeService.class);
    private static final String LIKE_EVENT_TOPIC = "like_event";
    private static final String POST_LIKE_COUNT_PREFIX = "post_like_count:";
    private static final String POST_LIKE_COUNT_LOCK_PREFIX = "post_like_count_lock:";
    private static final Duration COUNT_CACHE_TTL = Duration.ofHours(24);
    private static final Duration COUNT_LOCK_TTL = Duration.ofSeconds(5);
    private static final Duration COUNT_LOCK_RETRY_DELAY = Duration.ofMillis(50);
    private static final int COUNT_LOCK_RETRY_ATTEMPTS = 5;
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int MAX_PAGE_SIZE = 100;

    LikeRepository likeRepository;
    PostDetailsRepository postDetailsRepository;
    CommentRepository commentRepository;
    KafkaSender<String, String> kafkaSender;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    R2dbcEntityTemplate r2dbcEntityTemplate;

    // Luồng like phải kiểm tra target trước khi ghi DB để không tạo like mồ côi.
    public Mono<LikeToggleResponse> like(String actorId, LikeRequest request) {
        String normalizedTargetType = normalizeAndValidateTargetType(request.targetType());
        validateRequiredIds(actorId, request.targetId());

        log.info("|LikeService|like|actorId={}|targetId={}|targetType={}", actorId, request.targetId(), normalizedTargetType);

        return likeRepository.findByActorIdAndTargetIdAndTargetType(actorId, request.targetId(), normalizedTargetType)
                .flatMap(existing -> unlikeExisting(request.targetId(), normalizedTargetType, existing))
                .switchIfEmpty(Mono.defer(() -> createLike(actorId, request.targetId(), normalizedTargetType)))
                .onErrorMap(error -> {
                    log.error("|LikeService|like|failed|actorId={}|targetId={}|targetType={}|error={}",
                            actorId, request.targetId(), normalizedTargetType, error.getMessage());
                    return error instanceof AppException
                            ? error
                            : new AppException(
                                    ErrorCode.LIKE_CREATE_FAILED,
                                    "Toggle like failed",
                                    error
                            );
                });
    }
    // Unlike là thao tác xóa quan hệ hiện có; nếu chưa like thì trả lỗi nghiệp vụ rõ ràng.
    private Mono<LikeToggleResponse> unlikeExisting(String targetId, String targetType, Like existing) {
        log.info("|LikeService|unlikeExisting|likeId={}|actorId={}|targetId={}|targetType={}",
                existing.getId(), existing.getActorId(), targetId, targetType);
        return ensurePostLikeCountCache(targetId, targetType)
                .then(likeRepository.delete(existing))
                .then(updatePostLikeCountCache(targetId, targetType, -1))
                .thenReturn(new LikeToggleResponse(targetId, targetType, false, existing.getId()))
                .doOnSuccess(response -> log.info("|LikeService|unlikeExisting|completed|likeId={}|targetId={}|targetType={}",
                        existing.getId(), targetId, targetType));
    }

    private Mono<LikeToggleResponse> createLike(String actorId, String targetId, String targetType) {
        log.info("|LikeService|createLike|actorId={}|targetId={}|targetType={}", actorId, targetId, targetType);
        return resolveTargetContext(targetId, targetType)
                .flatMap(targetContext -> {
                    Instant now = Instant.now();
                    Like like = Like.builder()
                            .id(UUID.randomUUID().toString())
                            .actorId(actorId)
                            .targetId(targetId)
                            .targetType(targetType)
                            .timestamp(now)
                            .build();

                    return ensurePostLikeCountCache(targetId, targetType)
                            .then(r2dbcEntityTemplate.insert(Like.class).using(like))
                            .flatMap(saved -> updatePostLikeCountCache(targetId, targetType, 1)
                                    .thenReturn(saved))
                            .flatMap(saved -> resolveLikeCount(targetId, targetType)
                                    .flatMap(likeCount -> {
                                        LikeEventPayload payload = new LikeEventPayload(
                                                actorId,
                                                targetId,
                                                targetType,
                                                targetContext.ownerId(),
                                                targetContext.postId(),
                                                targetContext.parentCommentId(),
                                                likeCount,
                                                now
                                        );
                                        return publishLikeEvent(payload)
                                                .thenReturn(new LikeToggleResponse(targetId, targetType, true, saved.getId()))
                                                .doOnSuccess(response -> log.info("|LikeService|createLike|completed|likeId={}|actorId={}|targetId={}|targetType={}|likeCount={}",
                                                        saved.getId(), actorId, targetId, targetType, likeCount));
                                    }));
                });
    }

    // API trạng thái cần nhẹ và idempotent để FE gọi thường xuyên khi render feed.
    public Mono<Boolean> hasLiked(String actorId, String targetId, String targetType) {
        String normalizedTargetType = normalizeAndValidateTargetType(targetType);
        validateRequiredIds(actorId, targetId);

        return likeRepository.existsByActorIdAndTargetIdAndTargetType(actorId, targetId, normalizedTargetType)
                .doOnSuccess(exists -> log.info("|LikeService|hasLiked|actorId={}|targetId={}|targetType={}|result={}",
                        actorId, targetId, normalizedTargetType, exists))
                .doOnError(error -> log.error("|LikeService|hasLiked|failed|actorId={}|targetId={}|targetType={}|error={}",
                        actorId, targetId, normalizedTargetType, error.getMessage()))
                .onErrorMap(error -> new AppException(
                        ErrorCode.LIKE_FETCH_FAILED,
                        String.format("Check like status failed: actorId=%s targetId=%s targetType=%s", actorId, targetId, normalizedTargetType),
                        error
                ));
    }

    // Count luôn dựa trên DB vì Redis/cache chưa được định nghĩa là source of truth cho like.
    public Mono<Long> countLikes(String targetId, String targetType) {
        String normalizedTargetType = normalizeAndValidateTargetType(targetType);
        validateTargetId(targetId);

        Mono<Long> countSource = EntityType.POST.name().equals(normalizedTargetType)
                ? getPostLikeCountFromCache(targetId)
                : likeRepository.countByTargetIdAndTargetType(targetId, normalizedTargetType);

        return countSource
                .doOnSuccess(count -> log.info("|LikeService|countLikes|targetId={}|targetType={}|count={}",
                        targetId, normalizedTargetType, count))
                .doOnError(error -> log.error("|LikeService|countLikes|failed|targetId={}|targetType={}|error={}",
                        targetId, normalizedTargetType, error.getMessage()))
                .onErrorMap(error -> new AppException(
                        ErrorCode.LIKE_FETCH_FAILED,
                        String.format("Count likes failed: targetId=%s targetType=%s", targetId, normalizedTargetType),
                        error
                ));
    }

    // Trả về targetId thay vì entity Like để không lộ persistence model ra use case danh sách.
    public Mono<PageResponse<String>> getLikedTargets(String actorId, String targetType, int page, int size) {
        String normalizedTargetType = normalizeAndValidateTargetType(targetType);
        validateActorId(actorId);

        int pageNumber = Math.max(page, 0);
        int pageSize = validatePageSize(size);
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        return likeRepository.countByActorIdAndTargetType(actorId, normalizedTargetType)
                .flatMap(totalElements -> likeRepository.findTargetIdByActorIdAndTargetType(actorId, normalizedTargetType, pageable)
                        .collectList()
                        .map(content -> PageResponse.of(content, pageNumber, totalElements, pageSize)))
                .doOnSuccess(response -> log.info("|LikeService|getLikedTargets|actorId={}|targetType={}|page={}|resultCount={}|totalElements={}",
                        actorId, normalizedTargetType, pageNumber, response.content().size(), response.totalElements()))
                .doOnError(error -> log.error("|LikeService|getLikedTargets|failed|actorId={}|targetType={}|error={}",
                        actorId, normalizedTargetType, error.getMessage()))
                .onErrorMap(error -> new AppException(
                        ErrorCode.LIKE_FETCH_FAILED,
                        String.format("Fetch liked targets failed: actorId=%s targetType=%s", actorId, normalizedTargetType),
                        error
                ));
    }

    // Hiện tại việc kiểm tra target tồn tại được mock bằng chính DB của module post/comment.
    public Mono<PageResponse<String>> getLikerActorIds(String targetId, String targetType, int page, int size) {
        String normalizedTargetType = normalizeAndValidateTargetType(targetType);
        validateTargetId(targetId);
        int pageNumber = Math.max(page, 0);
        int pageSize = validatePageSize(size);
        Pageable pageable = PageRequest.of(pageNumber, pageSize);

        return likeRepository.countByTargetIdAndTargetType(targetId, normalizedTargetType)
                .flatMap(totalElements -> likeRepository
                        .findActorIdsByTargetIdAndTargetType(targetId, normalizedTargetType, pageable)
                        .collectList()
                        .map(content -> PageResponse.of(content, pageNumber, totalElements, pageSize)))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.LIKE_FETCH_FAILED, "Fetch liker actors failed", error));
    }
    private Mono<TargetContext> resolveTargetContext(String targetId, String targetType) {
        if (EntityType.POST.name().equals(targetType)) {
            return postDetailsRepository.findById(targetId)
                    .map(post -> new TargetContext(post.getUserId(), post.getPostId(), null))
                    .switchIfEmpty(Mono.error(new AppException(
                            ErrorCode.TARGET_NOT_FOUND,
                            String.format("Target not found: targetId=%s targetType=%s", targetId, targetType)
                    )));
        }

        return commentRepository.findById(targetId)
                .map(comment -> new TargetContext(comment.getUserId(), comment.getPostId(), comment.getParentId()))
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.TARGET_NOT_FOUND,
                        String.format("Target not found: targetId=%s targetType=%s", targetId, targetType)
                )));
    }

    private Mono<Long> resolveLikeCount(String targetId, String targetType) {
        return EntityType.POST.name().equals(targetType)
                ? countLikes(targetId, targetType)
                : likeRepository.countByTargetIdAndTargetType(targetId, targetType);
    }
    // KafkaSender reactive được trả về chain, không subscribe trong service.
    private Mono<Void> publishLikeEvent(LikeEventPayload payload) {
        JsonObject event = new JsonObject();
        event.addProperty("actorId", payload.actorId());
        event.addProperty("targetId", payload.targetId());
        event.addProperty("targetType", payload.targetType());
        event.addProperty("targetOwnerId", payload.targetOwnerId());
        event.addProperty("postId", payload.postId());
        event.addProperty("parentCommentId", payload.parentCommentId());
        event.addProperty("likeCount", payload.likeCount());
        event.addProperty("timestamp", payload.timestamp() == null ? null : payload.timestamp().toString());

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(LIKE_EVENT_TOPIC, payload.targetId(), event.toString()),
                LIKE_EVENT_TOPIC
        );

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|LikeService|publishLikeEvent|targetId={}|error={}", payload.targetId(), error.getMessage()))
                .doOnComplete(() -> log.info("|LikeService|publishLikeEvent|sent|actorId={}|targetId={}|targetType={}|postId={}|likeCount={}",
                        payload.actorId(), payload.targetId(), payload.targetType(), payload.postId(), payload.likeCount()))
                .then();
    }

    private Mono<Long> getPostLikeCountFromCache(String postId) {
        String cacheKey = postLikeCountKey(postId);
        return readLongCache(cacheKey)
                .doOnNext(count -> log.info("|LikeService|getPostLikeCountFromCache|cache hit|postId={}|count={}", postId, count))
                .switchIfEmpty(Mono.defer(() -> withCountLock(POST_LIKE_COUNT_LOCK_PREFIX + postId,
                        () -> readLongCache(cacheKey)
                                .doOnNext(count -> log.info("|LikeService|getPostLikeCountFromCache|cache hit after lock|postId={}|count={}", postId, count))
                                .switchIfEmpty(loadAndSetCountCache(cacheKey,
                                        () -> likeRepository.countByTargetIdAndTargetType(postId, EntityType.POST.name()))),
                        ErrorCode.LIKE_FETCH_FAILED,
                        String.format("Load post like count cache failed for postId=%s", postId)
                )));
    }

    private Mono<Void> ensurePostLikeCountCache(String targetId, String targetType) {
        if (!EntityType.POST.name().equals(targetType)) {
            return Mono.empty();
        }

        String cacheKey = postLikeCountKey(targetId);
        return withCountLock(POST_LIKE_COUNT_LOCK_PREFIX + targetId,
                () -> readLongCache(cacheKey)
                        .switchIfEmpty(Mono.defer(() -> loadAndSetCountCache(cacheKey,
                                () -> likeRepository.countByTargetIdAndTargetType(targetId, EntityType.POST.name()))))
                        .then(),
                ErrorCode.LIKE_FETCH_FAILED,
                String.format("Ensure post like count cache failed for postId=%s", targetId)
        );
    }

    private Mono<Void> updatePostLikeCountCache(String targetId, String targetType, long delta) {
        if (!EntityType.POST.name().equals(targetType)) {
            return Mono.empty();
        }

        String cacheKey = postLikeCountKey(targetId);
        return withCountLock(POST_LIKE_COUNT_LOCK_PREFIX + targetId,
                () -> reactiveRedisStringTemplate.opsForValue().increment(cacheKey, delta)
                        .flatMap(value -> value < 0
                                ? reactiveRedisStringTemplate.opsForValue().set(cacheKey, "0", COUNT_CACHE_TTL).then()
                                : reactiveRedisStringTemplate.expire(cacheKey, COUNT_CACHE_TTL).then()),
                ErrorCode.LIKE_FETCH_FAILED,
                String.format("Update post like count cache failed for postId=%s", targetId)
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
                .doOnSuccess(count -> log.info("|LikeService|loadAndSetCountCache|database count loaded|cacheKey={}|count={}",
                        cacheKey, count))
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
                        return Mono.error(new AppException(ErrorCode.LIKE_FETCH_FAILED, "Cannot acquire count cache lock"));
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

    private String postLikeCountKey(String postId) {
        return POST_LIKE_COUNT_PREFIX + postId;
    }

    private String normalizeAndValidateTargetType(String targetType) {
        if (targetType == null || targetType.isBlank()) {
            throw new AppException(ErrorCode.INVALID_TARGET_TYPE);
        }

        String normalized = targetType.trim().toUpperCase();
        if (!EntityType.POST.name().equals(normalized) && !EntityType.COMMENT.name().equals(normalized)) {
            throw new AppException(ErrorCode.INVALID_TARGET_TYPE);
        }
        return normalized;
    }

    private void validateRequiredIds(String actorId, String targetId) {
        validateActorId(actorId);
        validateTargetId(targetId);
    }

    private void validateActorId(String actorId) {
        if (actorId == null || actorId.isBlank()) {
            throw new AppException(ErrorCode.LIKE_CREATE_FAILED, "actorId is required");
        }
    }

    private void validateTargetId(String targetId) {
        if (targetId == null || targetId.isBlank()) {
            throw new AppException(ErrorCode.LIKE_CREATE_FAILED, "targetId is required");
        }
    }

    private int validatePageSize(int size) {
        if (size <= 0) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(size, MAX_PAGE_SIZE);
    }
    private record TargetContext(String ownerId, String postId, String parentCommentId) {
    }

}

