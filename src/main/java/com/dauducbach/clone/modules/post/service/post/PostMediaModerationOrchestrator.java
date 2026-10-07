package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetLifecycle;
import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repository.PostItemRepository;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PostMediaModerationOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(PostMediaModerationOrchestrator.class);
    private static final String POST_CACHE_PREFIX = "post:details:v3:";
    private static final String STATUS_PENDING = "PENDING_SCAN";
    private static final String STATUS_PROCESSING = "PROCESSING_SCAN";

    private final PostDetailsRepository postDetailsRepository;
    private final MediaAssetLifecycle mediaAssets;
    private final PostItemRepository postItemRepository;
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final PostSseService postSseService;
    private final KafkaSender<String, String> kafkaSender;
    private final MediaModerationProvider moderationProvider;
    private final PostVectorService postVectorService;
    private final R2dbcEntityTemplate r2dbcEntityTemplate;

    public Mono<Void> process(String postId, String userId, List<PostMediaScanItem> items) {
        if (postId == null || postId.isBlank() || userId == null || userId.isBlank()
                || items == null || items.isEmpty()) {
            return Mono.empty();
        }
        return claimPendingPost(postId)
                .doOnNext(claimed -> {
                    if (claimed) {
                        log.info("|PostMediaModerationOrchestrator|process|claimed|postId={}|itemCount={}", postId, items.size());
                    } else {
                        log.warn("|PostMediaModerationOrchestrator|process|skipped|postId={}|reason=not_pending_or_already_claimed", postId);
                    }
                })
                .flatMap(claimed -> claimed
                        ? processClaimedPost(postId, userId, items)
                                .doOnSuccess(unused -> log.info(
                                        "|PostMediaModerationOrchestrator|process|completed|postId={}", postId))
                                .doOnError(error -> log.error(
                                        "|PostMediaModerationOrchestrator|process|discarding|postId={}|error={}", postId, error.getMessage()))
                                .onErrorResume(error -> discardFailedPost(postId, items)
                                        .onErrorResume(cleanupError -> recordSecondaryFailure(postId, "cleanup", error, cleanupError))
                                        .then(Mono.defer(() -> sendPostFailureSse(userId, postId,
                                                "Bài viết không thể xử lý media, vui lòng thử lại"))
                                                .onErrorResume(notificationError -> recordSecondaryFailure(
                                                        postId, "failureNotification", error, notificationError)))
                                        .then(Mono.error(error)))
                        : Mono.empty())
                .doOnError(error -> log.error(
                        "|PostMediaModerationOrchestrator|process|failed|postId={}|error={}",
                        postId, error.getMessage()));
    }

    public Mono<Void> scanAdditionalPostItems(String postId, List<PostMediaScanItem> items) {
        if (items == null || items.isEmpty()) {
            return Mono.empty();
        }
        return scanAndPersistItems(postId, items).then();
    }

    private Mono<Boolean> claimPendingPost(String postId) {
        return postDetailsRepository.claimPendingMediaScan(
                        postId, STATUS_PENDING, STATUS_PROCESSING, Instant.now())
                .map(updated -> updated > 0)
                .defaultIfEmpty(false);
    }

    private Mono<Void> processClaimedPost(String postId, String userId, List<PostMediaScanItem> items) {
        return postItemRepository.deleteByPostId(postId)
                .then(scanAndPersistItems(postId, items))
                .flatMap(outcomes -> finalizePostScan(postId, userId, outcomes));
    }

    private Mono<List<PostScanOutcome>> scanAndPersistItems(String postId, List<PostMediaScanItem> items) {
        return Flux.fromIterable(items.stream()
                        .sorted(Comparator.comparing(PostMediaScanItem::getOrderNumber))
                        .toList())
                .concatMap(item -> scanAndSavePostItem(postId, item))
                .collectList();
    }

    private Mono<PostScanOutcome> scanAndSavePostItem(String postId, PostMediaScanItem item) {
        return moderationProvider.scan(item.getSecureUrl(), item.getPublicId(), item.getResourceType())
                .doOnSubscribe(subscription -> log.info(
                        "|PostMediaModerationOrchestrator|scanAndSavePostItem|started|postId={}|publicId={}|resourceType={}",
                        postId, item.getPublicId(), item.getResourceType()))
                .doOnNext(decision -> log.info(
                        "|PostMediaModerationOrchestrator|scanAndSavePostItem|decision|postId={}|publicId={}|decision={}",
                        postId, item.getPublicId(), decision))
                .flatMap(decision -> {
                    if (decision == MediaModerationProvider.Decision.REJECTED) {
                        return mediaAssets.deleteAsset(item.getPublicId())
                                .thenReturn(PostScanOutcome.rejected(item, "NSFW"));
                    }
                    return mediaAssets.fetchRemoteAsset(item.getPublicId())
                            .flatMap(media -> persistAllowedItem(postId, item, media));
                })
                .doOnError(error -> log.error(
                        "|PostMediaModerationOrchestrator|scanAndSavePostItem|postId={}|publicId={}|error={}",
                        postId, item.getPublicId(), error.getMessage()));
    }

    private Mono<PostScanOutcome> persistAllowedItem(
            String postId,
            PostMediaScanItem item,
            MediaAssetView media
    ) {
        if (!moderationProvider.isAllowedAsset(media)) {
            return mediaAssets.deleteAsset(item.getPublicId())
                    .thenReturn(PostScanOutcome.rejected(item, "INVALID_MEDIA"));
        }
        return mediaAssets.registerFetchedAsset(media, postId, OwnerType.POST)
                // The UUID is assigned before persistence: save() would attempt an UPDATE.
                .flatMap(savedMedia -> r2dbcEntityTemplate.insert(PostItem.class)
                        .using(buildPostItem(postId, item, savedMedia))
                        .thenReturn(PostScanOutcome.approved(item)));
    }

    private PostItem buildPostItem(String postId, PostMediaScanItem item, MediaAssetView media) {
        boolean video = "video".equalsIgnoreCase(media.resourceType());
        String itemMusicId = video ? null : normalizeOptional(item.getMusicId());
        Instant now = Instant.now();
        return PostItem.builder()
                .id(UUID.randomUUID().toString())
                .postId(postId)
                .orderNumber(item.getOrderNumber())
                .mediaId(media.assetId())
                .caption(normalizeOptional(item.getCaption()))
                .musicId(itemMusicId)
                .musicStart(itemMusicId == null ? null : item.getMusicStart())
                .musicEnd(itemMusicId == null ? null : item.getMusicEnd())
                .createdAt(now)
                .updatedAt(now)
                .build();
    }

    private Mono<Void> finalizePostScan(
            String postId,
            String userId,
            List<PostScanOutcome> outcomes
    ) {
        long rejectedCount = outcomes.stream().filter(outcome -> !outcome.approved()).count();
        long approvedCount = outcomes.size() - rejectedCount;
        log.info("|PostMediaModerationOrchestrator|finalizePostScan|postId={}|approvedCount={}|rejectedCount={}",
                postId, approvedCount, rejectedCount);
        return postDetailsRepository.findById(postId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.POST_NOT_FOUND,
                        "Post not found for scan result")))
                .flatMap(post -> {
                    if (approvedCount <= 0) {
                        return cleanupFailedPost(postId, outcomes)
                                .then(sendPostFailureSse(
                                        userId,
                                        postId,
                                        "Bài viết không được đăng tải vì toàn bộ nội dung vi phạm tiêu chuẩn cộng đồng"));
                    }

                    post.setValidateStatus("APPROVED");
                    post.setUpdatedAt(Instant.now());
                    String message = rejectedCount == 0
                            ? "Bài viết được đăng tải thành công"
                            : String.format(
                                    "Bài viết được đăng tải thành công, có %d ảnh/video bị xóa do vi phạm tiêu chuẩn cộng đồng",
                                    rejectedCount);
                    return postDetailsRepository.save(post)
                            .flatMap(saved -> sendPostUploadEvent(saved)
                                    .then(Mono.defer(() -> sendPostSuccessSse(userId, saved, message))));
                });
    }

    private Mono<Void> cleanupFailedPost(String postId, List<PostScanOutcome> outcomes) {
        return discardFailedPost(postId, outcomes.stream().map(PostScanOutcome::item).toList());
    }

    /** Compensate failed creation using all uploaded inputs, including items not yet scanned. */
    public Mono<Void> discardFailedPost(String postId, List<PostMediaScanItem> items) {
        List<String> publicIds = items.stream()
                .map(PostMediaScanItem::getPublicId)
                .filter(publicId -> publicId != null && !publicId.isBlank())
                .distinct()
                .toList();

        return Mono.defer(() -> postDetailsRepository.findById(postId))
                .doOnSubscribe(subscription -> log.info(
                        "|PostMediaModerationOrchestrator|discardFailedPost|started|postId={}|assetCount={}", postId, publicIds.size()))
                .map(java.util.Optional::of)
                .onErrorResume(error -> {
                    log.error("|PostMediaModerationOrchestrator|discardFailedPost|lookup|postId={}|error={}",
                            postId, error.getMessage());
                    return Mono.empty();
                })
                .defaultIfEmpty(java.util.Optional.empty())
                .flatMap(source -> {
                    // Each best-effort step runs even when another cleanup dependency fails.
                    // Remote deletion must run even if creating/finding the post failed.
                    return cleanupStep(postId, "cloudinary", () -> mediaAssets.deleteAssets(publicIds))
                            .then(cleanupStep(postId, "items", () -> postItemRepository.deleteByPostId(postId)))
                            .then(cleanupStep(postId, "media", () -> mediaAssets.deleteAssetsForOwner(postId, OwnerType.POST)))
                            .then(cleanupStep(postId, "cache", () -> redisTemplate.opsForValue()
                                    .delete(POST_CACHE_PREFIX + postId).then()))
                            .then(Mono.defer(() -> postDetailsRepository.deleteById(postId)))
                            // Fence only after SQL deletion succeeds.
                            .then(Mono.defer(() -> source.isPresent()
                                    ? postVectorService.deletePost(postId, source.get().getUserId())
                                    : postVectorService.rebuild(postId)));
                });
    }

    private Mono<Void> cleanupStep(String postId, String step, java.util.function.Supplier<Mono<Void>> action) {
        return Mono.defer(action).doOnSuccess(unused -> log.info(
                "|PostMediaModerationOrchestrator|discardFailedPost|stepCompleted|step={}|postId={}", step, postId))
                .onErrorResume(error -> {
                    log.error("|PostMediaModerationOrchestrator|discardFailedPost|step={}|postId={}|error={}",
                            step, postId, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> recordSecondaryFailure(String postId, String step, Throwable original, Throwable secondary) {
        if (original != secondary) original.addSuppressed(secondary);
        log.error("|PostMediaModerationOrchestrator|process|step={}|postId={}|error={}",
                step, postId, secondary.getMessage());
        return Mono.empty();
    }

    private Mono<Void> sendPostSuccessSse(String userId, PostDetails post, String message) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        JsonObject response = new JsonObject();
        response.addProperty("postId", post.getPostId());
        response.addProperty("result", "SUCCESSED");
        response.addProperty("message", message);
        return postSseService.sendToUser(userId, "post_upload", response.toString());
    }

    private Mono<Void> sendPostFailureSse(String userId, String postId, String message) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        JsonObject response = new JsonObject();
        response.addProperty("postId", postId);
        response.addProperty("result", "FAILED");
        response.addProperty("message", message);
        return postSseService.sendToUser(userId, "post_upload", response.toString());
    }

    private Mono<Void> sendPostUploadEvent(PostDetails post) {
        JsonObject payload = new JsonObject();
        payload.addProperty("post_id", post.getPostId());
        payload.addProperty("userId", post.getUserId());
        payload.addProperty("content", post.getContent());
        payload.add("hashtag", GsonUtils.getGson().toJsonTree(post.getHashtagList()));
        if (post.getMusicId() != null && !post.getMusicId().isBlank()) {
            payload.addProperty("musicId", post.getMusicId());
            payload.addProperty("musicStart", post.getMusicStart());
            payload.addProperty("musicEnd", post.getMusicEnd());
        }
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>("post_upload_event", post.getPostId(), payload.toString()),
                "post_upload_event");
        return kafkaSender.send(Mono.just(record))
                .flatMap(result -> result.exception() == null ? Mono.just(result) : Mono.error(result.exception()))
                .doOnError(error -> log.error(
                        "|PostMediaModerationOrchestrator|sendPostUploadEvent|postId={}|error={}",
                        post.getPostId(), error.getMessage()))
                .then();
    }

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }

    private record PostScanOutcome(PostMediaScanItem item, boolean approved, String reason) {
        static PostScanOutcome approved(PostMediaScanItem item) {
            return new PostScanOutcome(item, true, null);
        }

        static PostScanOutcome rejected(PostMediaScanItem item, String reason) {
            return new PostScanOutcome(item, false, reason);
        }

    }
}
