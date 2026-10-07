package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.application.PostDetailsCache;
import com.dauducbach.clone.modules.post.application.PostNotificationMuteStore;
import com.dauducbach.clone.modules.post.application.PostPublicationMessaging;
import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.post.dto.request.PostCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostItemUpdateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostUpdateRequest;
import com.dauducbach.clone.modules.post.dto.response.PostCreateResponse;
import com.dauducbach.clone.modules.post.dto.response.PostNotificationMuteResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.post.publishing.PostPublicationPolicy;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repository.PostItemRepository;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
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
    private static final long POST_NOTIFICATION_MUTE_DAYS = 60L;
    private static final String PENDING_SCAN_MESSAGE = "BÃ i viáº¿t máº¥t má»™t chÃºt thá»i gian Ä‘á»ƒ táº£i lÃªn, vui lÃ²ng Ä‘á»£i";

    PostDetailsRepository postDetailsRepository;
    PostItemRepository postItemRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    PostDetailsCache postDetailsCache;
    PostNotificationMuteStore postNotificationMuteStore;
    PostPublicationMessaging publicationMessaging;
    PostMediaModerationOrchestrator postMediaModerationOrchestrator;
    PostVectorService postVectorService;

    public Mono<PostCreateResponse> createPost(PostCreateRequest request) {
        return Mono.defer(() -> {
            PostPublicationPolicy.validateCreateRequest(request);

            String postId = UUID.randomUUID().toString();
            String userId = PostPublicationPolicy.normalizeRequired(request.getUserId(), "userId is required");
            List<PostMediaScanItem> scanItems = PostPublicationPolicy.buildScanItems(request);
            boolean hasMedia = !scanItems.isEmpty();
            String commonMusicId = PostPublicationPolicy.normalizeOptional(request.getMusicId());
            Long commonMusicStart = commonMusicId == null ? null : request.getMusicStart();
            Long commonMusicEnd = commonMusicId == null ? null : request.getMusicEnd();
            String mediaRatio = PostPublicationPolicy.normalizeRatio(request.getMediaRatio());
            String content = PostPublicationPolicy.sanitizeContent(request.getContent(), hasMedia);

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
                            ? publicationMessaging.requestMediaScan(saved.getPostId(), saved.getUserId(), scanItems)
                            : publicationMessaging.publishApproved(saved, "BÃ i viáº¿t Ä‘Ã£ Ä‘Æ°á»£c Ä‘Äƒng táº£i thÃ nh cÃ´ng"))
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
        String postId = PostPublicationPolicy.normalizeRequired(request.getPostId(), "postId is required");
        String actorId = PostPublicationPolicy.normalizeRequired(request.getUserId(), "userId is required");

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
                                PostPublicationPolicy.applyMetadataUpdate(existing, request, !existingItems.isEmpty());
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
            String requestedId = PostPublicationPolicy.normalizeOptional(requested.getItemId());
            if (requestedId != null && !existingById.containsKey(requestedId)) {
                throw new AppException(ErrorCode.POST_UPDATE_FAILED, "Post item does not belong to this post");
            }
            if (requestedId == null && (PostPublicationPolicy.normalizeOptional(requested.getSecureUrl()) == null
                    || PostPublicationPolicy.normalizeOptional(requested.getPublicId()) == null)) {
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
                .filter(requested -> PostPublicationPolicy.normalizeOptional(requested.getItemId()) != null)
                .index()
                .concatMap(indexed -> {
                    PostItemUpdateRequest requested = indexed.getT2();
                    PostItem item = existingById.get(requested.getItemId());
                    int orderNumber = requested.getOrderNumber() == null || requested.getOrderNumber() <= 0
                            ? Math.toIntExact(indexed.getT1() + 1)
                            : requested.getOrderNumber();
                    item.setOrderNumber(orderNumber);
                    item.setCaption(PostPublicationPolicy.normalizeOptional(requested.getCaption()));
                    String musicId = PostPublicationPolicy.normalizeOptional(requested.getMusicId());
                    if (musicId != null) {
                        PostPublicationPolicy.validateMusicSegment(musicId, requested.getMusicStart(), requested.getMusicEnd(), "post item");
                    }
                    item.setMusicId(musicId);
                    item.setMusicStart(musicId == null ? null : requested.getMusicStart());
                    item.setMusicEnd(musicId == null ? null : requested.getMusicEnd());
                    item.setUpdatedAt(Instant.now());
                    return postItemRepository.save(item);
                })
                .then();

        List<PostMediaScanItem> newItems = IntStream.range(0, requestedItems.size())
                .filter(index -> PostPublicationPolicy.normalizeOptional(requestedItems.get(index).getItemId()) == null)
                .mapToObj(index -> {
                    PostItemUpdateRequest requested = requestedItems.get(index);
                    String musicId = PostPublicationPolicy.normalizeOptional(requested.getMusicId());
                    if (musicId != null) {
                        PostPublicationPolicy.validateMusicSegment(musicId, requested.getMusicStart(), requested.getMusicEnd(), "post item");
                    }
                    return PostMediaScanItem.builder()
                            .orderNumber(requested.getOrderNumber() == null || requested.getOrderNumber() <= 0
                                    ? index + 1
                                    : requested.getOrderNumber())
                            .secureUrl(PostPublicationPolicy.normalizeRequired(requested.getSecureUrl(), "secureUrl is required"))
                            .publicId(PostPublicationPolicy.normalizeRequired(requested.getPublicId(), "publicId is required"))
                            .resourceType(PostPublicationPolicy.normalizeOptional(requested.getResourceType()))
                            .caption(PostPublicationPolicy.normalizeOptional(requested.getCaption()))
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

    private Mono<Void> refreshPostAfterUpdate(PostDetails updated) {
        return postDetailsCache.put(updated)
                .onErrorReturn(false)
                .then(publicationMessaging.publishUpdated(updated));
    }

    public Mono<PostNotificationMuteResponse> mutePostNotifications(String postId, String userId) {
        validatePostNotificationMuteRequest(postId, userId);

        log.info("|PostService|mutePostNotifications|postId={}|userId={}", postId, userId);
        return postDetailsRepository.existsById(postId)
                .flatMap(exists -> {
                    if (!Boolean.TRUE.equals(exists)) {
                        return Mono.error(new AppException(
                                ErrorCode.POST_NOT_FOUND,
                                String.format("Post not found for postId=%s", postId)
                        ));
                    }

                    return postNotificationMuteStore.mute(postId, userId)
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

    public Mono<Void> deletePostById(String postId, String userId) {
        log.info("|PostService|deletePostById|postId={}|userId={}", postId, userId);
        return postDetailsRepository.findById(postId)
                .map(java.util.Optional::of).defaultIfEmpty(java.util.Optional.empty())
                .flatMap(found -> {
                    if (found.isEmpty()) return postVectorService.retryDeletedPost(postId, userId)
                            .then(postDetailsCache.evict(postId));
                    PostDetails existing = found.get();
                    if (!userId.equals(existing.getUserId())) {
                        return Mono.error(new AppException(
                                ErrorCode.POST_DELETE_FAILED,
                                String.format("Only the post owner can delete postId=%s", postId)
                        ));
                    }
                    return postDetailsRepository.deleteById(postId)
                            .then(postDetailsCache.evict(postId))
                            .then(Mono.defer(() -> postVectorService.deletePost(postId, userId)));
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
                    Mono<Void> cacheRemoval = postDetailsCache.evictAll(posts.stream().map(PostDetails::getPostId).toList());

                    return cacheRemoval.then(Mono.defer(() -> postDetailsRepository.deleteByUserId(userId)))
                            .then(Mono.defer(() -> postVectorService.deletePostsByAuthor(userId, posts.stream().map(PostDetails::getPostId).toList())));
                })
                .doOnSuccess(v -> log.info("|PostService|deletePostsByUserId|deleted|userId={}", userId))
                .doOnError(error -> log.error("|PostService|deletePostsByUserId|failed|userId={}|error={}", userId, error.getMessage()))
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.POST_DELETE_FAILED,
                        String.format("Delete posts failed for userId=%s", userId),
                        throwable
                ));
    }

    private void validatePostNotificationMuteRequest(String postId, String userId) {
        if (postId == null || postId.isBlank()) {
            throw new AppException(ErrorCode.POST_NOTIFICATION_MUTE_FAILED, "postId is required");
        }
        if (userId == null || userId.isBlank()) {
            throw new AppException(ErrorCode.POST_NOTIFICATION_MUTE_FAILED, "userId is required");
        }
    }
}
