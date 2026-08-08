package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.constant.PostNotificationCacheKeys;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.constant.PostMediaRatio;
import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.post.dto.request.PostCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostItemCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostItemUpdateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostUpdateRequest;
import com.dauducbach.clone.modules.post.dto.response.PostCreateResponse;
import com.dauducbach.clone.modules.post.dto.response.PostNotificationMuteResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repositoty.PostItemRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.RedisUtil;
import com.google.gson.JsonObject;
import lombok.AccessLevel;
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
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.UUID;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class PostService {
    private static final Logger log = LoggerFactory.getLogger(PostService.class);
    private static final String POST_CACHE_PREFIX = "post:details:v3:";
    private static final Duration CACHE_TTL = Duration.ofHours(24);
    private static final Duration POST_NOTIFICATION_MUTE_TTL = Duration.ofDays(60);
    private static final long POST_NOTIFICATION_MUTE_DAYS = 60L;
    private static final String PENDING_SCAN_MESSAGE = "BÃ i viáº¿t máº¥t má»™t chÃºt thá»i gian Ä‘á»ƒ táº£i lÃªn, vui lÃ²ng Ä‘á»£i";

    PostDetailsRepository postDetailsRepository;
    PostItemRepository postItemRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    KafkaSender<String, String> kafkaSender;
    PostSseService postSseService;
    PostMediaModerationOrchestrator postMediaModerationOrchestrator;

    public Mono<PostCreateResponse> createPost(PostCreateRequest request) {
        return Mono.defer(() -> {
            validateCreateRequest(request);

            String postId = UUID.randomUUID().toString();
            String userId = normalizeRequired(request.getUserId(), "userId is required");
            List<PostMediaScanItem> scanItems = buildScanItems(request);
            boolean hasMedia = !scanItems.isEmpty();
            String commonMusicId = normalizeOptional(request.getMusicId());
            Long commonMusicStart = commonMusicId == null ? null : request.getMusicStart();
            Long commonMusicEnd = commonMusicId == null ? null : request.getMusicEnd();
            String mediaRatio = PostMediaRatio.defaultIfMissing(request.getMediaRatio());
            String content = sanitizeContent(request.getContent(), hasMedia);

            PostDetails postDetails = PostDetails.builder()
                    .postId(postId)
                    .userId(userId)
                    .content(content)
                    .musicId(commonMusicId)
                    .musicStart(commonMusicStart)
                    .musicEnd(commonMusicEnd)
                    .mediaRatio(mediaRatio)
                    .validateStatus(hasMedia ? "PENDING_SCAN" : "APPROVED")
                    .build();
            postDetails.setCreatedAt(Instant.now());
            postDetails.setUpdatedAt(Instant.now());
            postDetails.setHashtagList(request.getHashtags());

            Mono<Void> createAction = r2dbcEntityTemplate.insert(PostDetails.class)
                    .using(postDetails)
                    .flatMap(saved -> hasMedia
                            ? sendCheckMediaEvent(saved.getPostId(), saved.getUserId(), scanItems)
                            : sendPostSuccessSse(saved, "BÃ i viáº¿t Ä‘Ã£ Ä‘Æ°á»£c Ä‘Äƒng táº£i thÃ nh cÃ´ng")
                                    .then(sendPostUploadEvent(saved)))
                    .doOnSuccess(v -> log.info("|PostService|createPost|accepted|postId={}|userId={}|mediaCount={}",
                            postId, userId, scanItems.size()))
                    .onErrorMap(throwable -> throwable instanceof AppException
                            ? throwable
                            : new AppException(
                                    ErrorCode.POST_CREATE_FAILED,
                                    String.format("Create post failed for userId=%s", userId),
                                    throwable
                            ));

            return createAction.thenReturn(PostCreateResponse.builder()
                    .postId(postId)
                    .message(hasMedia ? PENDING_SCAN_MESSAGE : "BÃ i viáº¿t Ä‘Ã£ Ä‘Æ°á»£c Ä‘Äƒng táº£i thÃ nh cÃ´ng")
                    .build());
        });
    }

    public Mono<PostDetails> updatePost(PostUpdateRequest request) {
        if (request == null) {
            return Mono.error(new AppException(ErrorCode.POST_UPDATE_FAILED, "Update request is required"));
        }
        String postId = normalizeRequired(request.getPostId(), "postId is required");
        String actorId = normalizeRequired(request.getUserId(), "userId is required");

        return postDetailsRepository.findById(postId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.POST_NOT_FOUND,
                        String.format("Post not found for postId=%s", postId)
                )))
                .flatMap(existing -> {
                    if (!actorId.equals(existing.getUserId())) {
                        return Mono.error(new AppException(
                                ErrorCode.POST_UPDATE_FAILED,
                                "Only the post owner can update this post"
                        ));
                    }
                    return postItemRepository.findByPostIdOrderByOrderNumberAsc(postId)
                            .collectList()
                            .flatMap(existingItems -> {
                                applyPostMetadataUpdate(existing, request, !existingItems.isEmpty());
                                return applyPostItemUpdates(postId, existingItems, request.getItems())
                                        .then(postDetailsRepository.save(existing));
                            });
                })
                .flatMap(updated -> refreshPostAfterUpdate(updated).thenReturn(updated))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.POST_UPDATE_FAILED,
                                String.format("Update post failed for postId=%s", postId),
                                throwable
                        ));
    }


    private Mono<Void> applyPostItemUpdates(
            String postId,
            List<PostItem> existingItems,
            List<PostItemUpdateRequest> requestedItems
    ) {
        if (requestedItems == null) {
            return Mono.empty();
        }
        Map<String, PostItem> existingById = existingItems.stream()
                .collect(Collectors.toMap(PostItem::getId, Function.identity()));
        for (PostItemUpdateRequest requested : requestedItems) {
            String requestedId = normalizeOptional(requested.getItemId());
            if (requestedId != null && !existingById.containsKey(requestedId)) {
                throw new AppException(ErrorCode.POST_UPDATE_FAILED, "Post item does not belong to this post");
            }
            if (requestedId == null && (normalizeOptional(requested.getSecureUrl()) == null
                    || normalizeOptional(requested.getPublicId()) == null)) {
                throw new AppException(ErrorCode.POST_UPDATE_FAILED, "New media upload data is incomplete");
            }
        }

        List<String> requestedIds = requestedItems.stream()
                .map(PostItemUpdateRequest::getItemId)
                .filter(itemId -> itemId != null && !itemId.isBlank())
                .toList();
        Mono<Void> removeOmitted = Flux.fromIterable(existingItems)
                .filter(item -> !requestedIds.contains(item.getId()))
                .concatMap(item -> postItemRepository.deleteById(item.getId()))
                .then();

        Mono<Void> updateKept = Flux.fromIterable(requestedItems)
                .filter(requested -> normalizeOptional(requested.getItemId()) != null)
                .index()
                .concatMap(indexed -> {
                    PostItemUpdateRequest requested = indexed.getT2();
                    PostItem item = existingById.get(requested.getItemId());
                    int orderNumber = requested.getOrderNumber() == null || requested.getOrderNumber() <= 0
                            ? Math.toIntExact(indexed.getT1() + 1)
                            : requested.getOrderNumber();
                    item.setOrderNumber(orderNumber);
                    item.setCaption(normalizeOptional(requested.getCaption()));
                    String musicId = normalizeOptional(requested.getMusicId());
                    if (musicId != null) {
                        validateMusicSegment(musicId, requested.getMusicStart(), requested.getMusicEnd(), "post item");
                    }
                    item.setMusicId(musicId);
                    item.setMusicStart(musicId == null ? null : requested.getMusicStart());
                    item.setMusicEnd(musicId == null ? null : requested.getMusicEnd());
                    item.setUpdatedAt(Instant.now());
                    return postItemRepository.save(item);
                })
                .then();

        List<PostMediaScanItem> newItems = IntStream.range(0, requestedItems.size())
                .filter(index -> normalizeOptional(requestedItems.get(index).getItemId()) == null)
                .mapToObj(index -> {
                    PostItemUpdateRequest requested = requestedItems.get(index);
                    String musicId = normalizeOptional(requested.getMusicId());
                    if (musicId != null) {
                        validateMusicSegment(musicId, requested.getMusicStart(), requested.getMusicEnd(), "post item");
                    }
                    return PostMediaScanItem.builder()
                            .orderNumber(requested.getOrderNumber() == null || requested.getOrderNumber() <= 0
                                    ? index + 1
                                    : requested.getOrderNumber())
                            .secureUrl(normalizeRequired(requested.getSecureUrl(), "secureUrl is required"))
                            .publicId(normalizeRequired(requested.getPublicId(), "publicId is required"))
                            .resourceType(normalizeOptional(requested.getResourceType()))
                            .caption(normalizeOptional(requested.getCaption()))
                            .musicId(musicId)
                            .musicStart(musicId == null ? null : requested.getMusicStart())
                            .musicEnd(musicId == null ? null : requested.getMusicEnd())
                            .build();
                })
                .toList();

        return postMediaModerationOrchestrator.scanAdditionalPostItems(postId, newItems)
                .then(removeOmitted)
                .then(updateKept)
                .doOnSuccess(ignored -> log.info(
                        "|PostService|applyPostItemUpdates|postId={}|kept={}|added={}|removed={}",
                        postId,
                        requestedIds.size(),
                        newItems.size(),
                        Math.max(0, existingItems.size() - requestedItems.size())
                ));
    }

    private void applyPostMetadataUpdate(PostDetails existing, PostUpdateRequest request, boolean hasExistingMedia) {
        if (request.getContent() != null) {
            existing.setContent(sanitizeContent(request.getContent(), hasExistingMedia || request.getItems() != null));
        }
        if (request.getHashtag() != null) {
            existing.setHashtagList(request.getHashtag());
        }
        if (request.getMediaRatio() != null) {
            if (!PostMediaRatio.isSupported(request.getMediaRatio())) {
                throw new AppException(ErrorCode.POST_UPDATE_FAILED, "Unsupported mediaRatio");
            }
            existing.setMediaRatio(PostMediaRatio.defaultIfMissing(request.getMediaRatio()));
        }
        if (request.getMusicId() != null || request.getMusicStart() != null || request.getMusicEnd() != null) {
            String musicId = normalizeOptional(request.getMusicId());
            validateMusicSegment(musicId, request.getMusicStart(), request.getMusicEnd(), "post");
            existing.setMusicId(musicId);
            existing.setMusicStart(musicId == null ? null : request.getMusicStart());
            existing.setMusicEnd(musicId == null ? null : request.getMusicEnd());
        }
        existing.setUpdatedAt(Instant.now());
    }

    private Mono<Void> refreshPostAfterUpdate(PostDetails updated) {
        String cacheKey = POST_CACHE_PREFIX + updated.getPostId();
        String cacheValue = RedisUtil.serialize(updated);
        Mono<Boolean> cacheUpdate = cacheValue == null
                ? Mono.just(false)
                : reactiveRedisStringTemplate.opsForValue().set(cacheKey, cacheValue, CACHE_TTL);
        return cacheUpdate
                .onErrorReturn(false)
                .then(publishPostEvent("post_update_event", updated));
    }

    public Mono<PostDetails> getPostById(String postId) {
        String cacheKey = POST_CACHE_PREFIX + postId;

        log.info("|PostService|getPostById|postId={}", postId);
        return reactiveRedisStringTemplate.opsForValue().get(cacheKey)
                .onErrorResume(error -> {
                    log.warn("|PostService|getPostById|cache read failed, fallback to database|postId={}|error={}", postId, error.getMessage());
                    return Mono.empty();
                })
                .flatMap(cacheValue -> {
                    PostDetails cached = RedisUtil.deserialize(cacheValue, PostDetails.class);
                    if (cached != null) {
                        log.info("|PostService|getPostById|cache hit|postId={}", postId);
                        return Mono.just(cached);
                    }
                    log.warn("|PostService|getPostById|cache deserialize failed|postId={}", postId);
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
                        .flatMap(post -> {
                            log.info("|PostService|getPostById|database hit|postId={}|userId={}", postId, post.getUserId());
                            String serialized = RedisUtil.serialize(post);
                            if (serialized == null) {
                                return Mono.just(post);
                            }
                            return reactiveRedisStringTemplate.opsForValue()
                                    .set(cacheKey, serialized, CACHE_TTL)
                                    .onErrorResume(error -> {
                                        log.warn("|PostService|getPostById|cache write failed|postId={}|error={}",
                                                postId, error.getMessage());
                                        return Mono.empty();
                                    })
                                    .thenReturn(post);
                        })
                );
    }

    public Mono<String> getPostOwnerIdByPostId(String postId) {
        if (postId == null || postId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.POST_FETCH_FAILED, "postId is required"));
        }

        log.info("|PostService|getPostOwnerIdByPostId|postId={}", postId);
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
                        ))
                .doOnSuccess(userId -> log.info("|PostService|getPostOwnerIdByPostId|success|postId={}|ownerId={}", postId, userId))
                .doOnError(error -> log.error("|PostService|getPostOwnerIdByPostId|failed|postId={}|error={}", postId, error.getMessage()));
    }

    public Mono<PostNotificationMuteResponse> mutePostNotifications(String postId, String userId) {
        validatePostNotificationMuteRequest(postId, userId);

        String cacheKey = PostNotificationCacheKeys.mutedPostNotification(postId, userId);
        log.info("|PostService|mutePostNotifications|postId={}|userId={}", postId, userId);
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
                .doOnError(error -> log.error("|PostService|mutePostNotifications|failed|postId={}|userId={}|error={}",
                        postId, userId, error.getMessage()))
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

        log.info("|PostService|getPostsByUserId|userId={}|page={}|size={}|limit={}|offset={}",
                userId, page, size, limit, offset);
        return postDetailsRepository.findByUserId(userId, limit, offset)
                .doOnComplete(() -> log.info("|PostService|getPostsByUserId|completed|userId={}|limit={}|offset={}",
                        userId, limit, offset))
                .doOnError(error -> log.error("|PostService|getPostsByUserId|failed|userId={}|error={}",
                        userId, error.getMessage()))
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.POST_LIST_FETCH_FAILED,
                        String.format("Fetch posts failed for userId=%s", userId),
                        throwable
                ));
    }

    public Mono<Void> deletePostById(String postId, String userId) {
        String cacheKey = POST_CACHE_PREFIX + postId;

        log.info("|PostService|deletePostById|postId={}|userId={}", postId, userId);
        return postDetailsRepository.findById(postId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.POST_NOT_FOUND,
                        String.format("Post not found for postId=%s", postId)
                )))
                .flatMap(existing -> {
                    if (!userId.equals(existing.getUserId())) {
                        return Mono.error(new AppException(
                                ErrorCode.POST_DELETE_FAILED,
                                String.format("Only the post owner can delete postId=%s", postId)
                        ));
                    }
                    return postDetailsRepository.deleteById(postId)
                            .then(reactiveRedisStringTemplate.opsForValue().delete(cacheKey).then());
                })
                .doOnSuccess(v -> log.info("|PostService|deletePostById|deleted|postId={}|userId={}", postId, userId))
                .doOnError(error -> log.error("|PostService|deletePostById|failed|postId={}|userId={}|error={}", postId, userId, error.getMessage()))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.POST_DELETE_FAILED,
                                String.format("Delete post failed for postId=%s", postId),
                                throwable
                        ));
    }

    public Mono<Void> deletePostsByUserId(String userId) {
        log.info("|PostService|deletePostsByUserId|userId={}", userId);
        return postDetailsRepository.findAllByUserId(userId)
                .collectList()
                .flatMap(posts -> {
                    log.info("|PostService|deletePostsByUserId|found posts|userId={}|count={}", userId, posts.size());
                    Mono<Void> cacheRemoval = Flux.fromIterable(posts)
                            .flatMap(post -> reactiveRedisStringTemplate.opsForValue()
                                    .delete(POST_CACHE_PREFIX + post.getPostId())
                                    .then())
                            .then();

                    return cacheRemoval.then(postDetailsRepository.deleteByUserId(userId));
                })
                .doOnSuccess(v -> log.info("|PostService|deletePostsByUserId|deleted|userId={}", userId))
                .doOnError(error -> log.error("|PostService|deletePostsByUserId|failed|userId={}|error={}", userId, error.getMessage()))
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.POST_DELETE_FAILED,
                        String.format("Delete posts failed for userId=%s", userId),
                        throwable
                ));
    }

    private void validateCreateRequest(PostCreateRequest request) {
        if (request == null) {
            throw new AppException(ErrorCode.POST_CREATE_FAILED, "Post request is required");
        }
        normalizeRequired(request.getUserId(), "userId is required");
        if (normalizeOptional(request.getMediaRatio()) != null && !PostMediaRatio.isSupported(request.getMediaRatio())) {
            throw new AppException(ErrorCode.POST_CREATE_FAILED,
                    "mediaRatio must be one of 1:1, 4:5, 3:4, 9:16, 4:3, 3:2, 16:9");
        }
        List<PostItemCreateRequest> items = request.getItems() == null ? List.of() : request.getItems();
        if (items.isEmpty() && normalizeOptional(request.getContent()) == null) {
            throw new AppException(ErrorCode.POST_CONTENT_INVALID, "Post content or media is required");
        }

        String commonMusicId = normalizeOptional(request.getMusicId());
        if (commonMusicId != null) {
            validateMusicSegment(commonMusicId, request.getMusicStart(), request.getMusicEnd(), "post");
        }

        for (int index = 0; index < items.size(); index++) {
            PostItemCreateRequest item = items.get(index);
            int orderNumber = item.getOrderNumber() == null || item.getOrderNumber() <= 0 ? index + 1 : item.getOrderNumber();
            normalizeRequired(item.getSecureUrl(), "secureUrl is required for post item " + orderNumber);
            normalizeRequired(item.getPublicId(), "publicId is required for post item " + orderNumber);
            if (commonMusicId == null) {
                String itemMusicId = normalizeOptional(item.getMusicId());
                if (itemMusicId != null && !isVideoItem(item)) {
                    validateMusicSegment(itemMusicId, item.getMusicStart(), item.getMusicEnd(), "post item " + orderNumber);
                }
            }
        }
    }

    private List<PostMediaScanItem> buildScanItems(PostCreateRequest request) {
        List<PostItemCreateRequest> items = request.getItems() == null ? List.of() : request.getItems();
        boolean useSharedMusic = normalizeOptional(request.getMusicId()) != null;
        return IntStream.range(0, items.size())
                .mapToObj(index -> {
                    PostItemCreateRequest item = items.get(index);
                    int orderNumber = item.getOrderNumber() == null || item.getOrderNumber() <= 0 ? index + 1 : item.getOrderNumber();
                    boolean videoItem = isVideoItem(item);
                    String itemMusicId = useSharedMusic || videoItem ? null : normalizeOptional(item.getMusicId());
                    return PostMediaScanItem.builder()
                            .orderNumber(orderNumber)
                            .secureUrl(normalizeRequired(item.getSecureUrl(), "secureUrl is required for post item " + orderNumber))
                            .publicId(normalizeRequired(item.getPublicId(), "publicId is required for post item " + orderNumber))
                            .resourceType(normalizeOptional(item.getResourceType()))
                            .caption(normalizeOptional(item.getCaption()))
                            .musicId(itemMusicId)
                            .musicStart(itemMusicId == null ? null : item.getMusicStart())
                            .musicEnd(itemMusicId == null ? null : item.getMusicEnd())
                            .build();
                })
                .sorted(Comparator.comparing(PostMediaScanItem::getOrderNumber))
                .toList();
    }

    private boolean isVideoItem(PostItemCreateRequest item) {
        String value = firstNonBlank(item.getResourceType(), item.getSecureUrl());
        if (value == null) {
            return false;
        }
        String lower = value.toLowerCase();
        return lower.contains("video") || lower.endsWith(".mp4") || lower.endsWith(".mov") || lower.endsWith(".webm") || lower.endsWith(".m4v");
    }

    private String firstNonBlank(String first, String second) {
        String normalizedFirst = normalizeOptional(first);
        return normalizedFirst != null ? normalizedFirst : normalizeOptional(second);
    }

    private void validateMusicSegment(String musicId, Long musicStart, Long musicEnd, String scope) {
        if (musicId == null) {
            if (musicStart != null || musicEnd != null) {
                throw new AppException(ErrorCode.POST_CREATE_FAILED,
                        "musicId is required when a music segment is provided for " + scope);
            }
            return;
        }
        if (musicStart == null || musicEnd == null) {
            throw new AppException(ErrorCode.POST_CREATE_FAILED,
                    "musicStart and musicEnd are required for " + scope);
        }
        if (musicStart < 0 || musicEnd <= musicStart) {
            throw new AppException(ErrorCode.POST_CREATE_FAILED,
                    "Invalid music segment for " + scope);
        }
    }

    private String sanitizeContent(String content, boolean allowEmpty) {
        String normalized = normalizeOptional(content);
        if (normalized == null) {
            if (allowEmpty) {
                return "";
            }
            throw new AppException(ErrorCode.POST_CONTENT_INVALID, "Post content is empty");
        }

        String sanitized = Jsoup.clean(normalized, Safelist.relaxed()).trim();
        if (sanitized.isBlank() && !allowEmpty) {
            throw new AppException(ErrorCode.POST_CONTENT_INVALID, "Post content is invalid after sanitization");
        }
        return sanitized;
    }

    private String normalizeRequired(String value, String message) {
        String normalized = normalizeOptional(value);
        if (normalized == null) {
            throw new AppException(ErrorCode.POST_CREATE_FAILED, message);
        }
        return normalized;
    }

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private void validatePostNotificationMuteRequest(String postId, String userId) {
        if (postId == null || postId.isBlank()) {
            throw new AppException(ErrorCode.POST_NOTIFICATION_MUTE_FAILED, "postId is required");
        }
        if (userId == null || userId.isBlank()) {
            throw new AppException(ErrorCode.POST_NOTIFICATION_MUTE_FAILED, "userId is required");
        }
    }

    private Mono<Void> publishPostEvent(String topic, PostDetails postDetails) {
        JsonObject payload = new JsonObject();
        payload.addProperty("post_id", postDetails.getPostId());
        payload.addProperty("content", postDetails.getContent());
        payload.addProperty("mediaRatio", PostMediaRatio.defaultIfMissing(postDetails.getMediaRatio()));
        payload.add("hashtag", GsonUtils.getGson().toJsonTree(postDetails.getHashtagList()));

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(topic, postDetails.getPostId(), payload.toString()),
                topic
        );

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|PostService|publishPostEvent|topic={}|error={}", topic, error.getMessage()))
                .doOnComplete(() -> log.info("|PostService|publishPostEvent|sent|topic={}|postId={}", topic, postDetails.getPostId()))
                .then();
    }

    private Mono<Void> sendPostUploadEvent(PostDetails postDetails) {
        JsonObject payload = buildPostEventPayload(postDetails);

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>("post_upload_event", postDetails.getPostId(), payload.toString()),
                "post_upload_event"
        );

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|PostService|sendPostUploadEvent|postId={}|error={}", postDetails.getPostId(), error.getMessage()))
                .doOnComplete(() -> log.info("|PostService|sendPostUploadEvent|sent|postId={}|userId={}",
                        postDetails.getPostId(), postDetails.getUserId()))
                .then();
    }

    private JsonObject buildPostEventPayload(PostDetails postDetails) {
        JsonObject payload = new JsonObject();
        payload.addProperty("post_id", postDetails.getPostId());
        payload.addProperty("userId", postDetails.getUserId());
        payload.addProperty("content", postDetails.getContent());
        payload.addProperty("mediaRatio", PostMediaRatio.defaultIfMissing(postDetails.getMediaRatio()));
        payload.add("hashtag", GsonUtils.getGson().toJsonTree(postDetails.getHashtagList()));
        if (postDetails.getMusicId() != null && !postDetails.getMusicId().isBlank()) {
            payload.addProperty("musicId", postDetails.getMusicId());
            payload.addProperty("musicStart", postDetails.getMusicStart());
            payload.addProperty("musicEnd", postDetails.getMusicEnd());
        }
        return payload;
    }

    private Mono<Void> sendPostSuccessSse(PostDetails postDetails, String message) {
        JsonObject payload = new JsonObject();
        payload.addProperty("postId", postDetails.getPostId());
        payload.addProperty("result", "SUCCESSED");
        payload.addProperty("message", message);
        return postSseService.sendToUser(
                        postDetails.getUserId(),
                        "post_upload",
                        payload.toString())
                .doOnSuccess(unused -> log.info(
                        "|PostService|sendPostSuccessSse|sent|postId={}|userId={}",
                        postDetails.getPostId(),
                        postDetails.getUserId()));
    }

    private Mono<Void> sendCheckMediaEvent(String postId, String userId, List<PostMediaScanItem> items) {
        JsonObject payload = new JsonObject();
        payload.addProperty("postId", postId);
        payload.addProperty("userId", userId);
        payload.add("items", GsonUtils.getGson().toJsonTree(items));

        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>("check_media_event", postId, payload.toString()),
                "check_media_event"
        );

        return kafkaSender.send(Mono.just(record))
                .doOnError(error -> log.error("|PostService|sendCheckMediaEvent|postId={}|error={}", postId, error.getMessage()))
                .doOnComplete(() -> log.info("|PostService|sendCheckMediaEvent|sent|postId={}|userId={}|itemCount={}",
                        postId, userId, items.size()))
                .then();
    }
}
