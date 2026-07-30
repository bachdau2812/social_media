package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaAssetCleanupService;
import com.dauducbach.clone.modules.media.service.MediaService;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repositoty.PostItemRepository;
import com.dauducbach.clone.utils.GsonUtils;
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
    private final MediaService mediaService;
    private final PostItemRepository postItemRepository;
    private final R2dbcEntityTemplate r2dbcEntityTemplate;
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final PostSseService postSseService;
    private final KafkaSender<String, String> kafkaSender;
    private final MediaCompatibilityFacade cloudinaryMediaService;
    private final MediaModerationProvider moderationProvider;
    private final MediaAssetCleanupService cleanupService;

    public Mono<Void> process(String postId, String userId, List<PostMediaScanItem> items) {
        if (postId == null || postId.isBlank() || userId == null || userId.isBlank()
                || items == null || items.isEmpty()) {
            return Mono.empty();
        }
        return claimPendingPost(postId)
                .flatMap(claimed -> claimed
                        ? processClaimedPost(postId, userId, items)
                        : Mono.empty())
                .doOnSuccess(unused -> log.info(
                        "|PostMediaModerationOrchestrator|process|completed|postId={}", postId))
                .doOnError(error -> log.error(
                        "|PostMediaModerationOrchestrator|process|failed|postId={}|error={}",
                        postId, error.getMessage()))
                .onErrorResume(error -> releaseClaim(postId)
                        .then(sendPostFailureSse(
                                userId,
                                postId,
                                "Bài viết không thể xử lý media, vui lòng thử lại"))
                        .then(Mono.error(error)));
    }

    public Mono<Void> scanAdditionalPostItems(String postId, List<PostMediaScanItem> items) {
        if (items == null || items.isEmpty()) {
            return Mono.empty();
        }
        return scanAndPersistItems(postId, items)
                .flatMap(outcomes -> outcomes.stream().anyMatch(PostScanOutcome::processingFailure)
                        ? Mono.error(new AppException(
                                ErrorCode.POST_UPDATE_FAILED,
                                "One or more new media items could not be processed"))
                        : Mono.empty());
    }

    private Mono<Boolean> claimPendingPost(String postId) {
        return r2dbcEntityTemplate.getDatabaseClient().sql("""
                        UPDATE post_details
                        SET validate_status = :processing, updated_at = :updatedAt
                        WHERE post_id = :postId AND validate_status = :pending
                        """)
                .bind("processing", STATUS_PROCESSING)
                .bind("updatedAt", Instant.now())
                .bind("postId", postId)
                .bind("pending", STATUS_PENDING)
                .fetch()
                .rowsUpdated()
                .map(updated -> updated > 0)
                .defaultIfEmpty(false);
    }

    private Mono<Void> releaseClaim(String postId) {
        return r2dbcEntityTemplate.getDatabaseClient().sql("""
                        UPDATE post_details
                        SET validate_status = :pending, updated_at = :updatedAt
                        WHERE post_id = :postId AND validate_status = :processing
                        """)
                .bind("pending", STATUS_PENDING)
                .bind("updatedAt", Instant.now())
                .bind("postId", postId)
                .bind("processing", STATUS_PROCESSING)
                .fetch()
                .rowsUpdated()
                .then();
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
        return moderationProvider.scan(item.getSecureUrl(), item.getPublicId())
                .flatMap(decision -> {
                    if (decision == MediaModerationProvider.Decision.REJECTED) {
                        return cleanupService.delete(item.getPublicId())
                                .thenReturn(PostScanOutcome.rejected(item, "NSFW"));
                    }
                    return cloudinaryMediaService.fetchMediaByPublicId(item.getPublicId())
                            .flatMap(media -> persistAllowedItem(postId, item, media));
                })
                .onErrorResume(error -> {
                    log.error(
                            "|PostMediaModerationOrchestrator|scanAndSavePostItem|postId={}|publicId={}|error={}",
                            postId, item.getPublicId(), error.getMessage());
                    return cleanupService.delete(item.getPublicId())
                            .thenReturn(PostScanOutcome.failed(item, "PROCESSING_ERROR"));
                });
    }

    private Mono<PostScanOutcome> persistAllowedItem(
            String postId,
            PostMediaScanItem item,
            Media media
    ) {
        if (!moderationProvider.isAllowedAsset(media)) {
            return cleanupService.delete(item.getPublicId())
                    .thenReturn(PostScanOutcome.rejected(item, "INVALID_MEDIA"));
        }
        return mediaService.registerFetchedMedia(media, postId, OwnerType.POST)
                .flatMap(savedMedia -> r2dbcEntityTemplate.insert(PostItem.class)
                        .using(buildPostItem(postId, item, savedMedia))
                        .thenReturn(PostScanOutcome.approved(item)));
    }

    private PostItem buildPostItem(String postId, PostMediaScanItem item, Media media) {
        boolean video = "video".equalsIgnoreCase(media.getResourceType());
        String itemMusicId = video ? null : normalizeOptional(item.getMusicId());
        Instant now = Instant.now();
        return PostItem.builder()
                .id(UUID.randomUUID().toString())
                .postId(postId)
                .orderNumber(item.getOrderNumber())
                .mediaId(media.getAssetId())
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
        long processingFailureCount = outcomes.stream()
                .filter(PostScanOutcome::processingFailure)
                .count();

        return postDetailsRepository.findById(postId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.POST_NOT_FOUND,
                        "Post not found for scan result")))
                .flatMap(post -> {
                    if (processingFailureCount > 0) {
                        return cleanupFailedPost(postId, outcomes)
                                .then(sendPostFailureSse(
                                        userId,
                                        postId,
                                        "Bài viết không thể xử lý, vui lòng thử lại"));
                    }
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
                            .flatMap(saved -> sendPostSuccessSse(userId, saved, message)
                                    .then(sendPostUploadEvent(saved)));
                });
    }

    private Mono<Void> cleanupFailedPost(String postId, List<PostScanOutcome> outcomes) {
        List<String> publicIds = outcomes.stream()
                .map(PostScanOutcome::item)
                .map(PostMediaScanItem::getPublicId)
                .filter(publicId -> publicId != null && !publicId.isBlank())
                .distinct()
                .toList();

        return cleanupService.deleteAll(publicIds)
                .then(postItemRepository.deleteByPostId(postId))
                .then(mediaService.deleteByOwnerIdAndOwnerType(postId, OwnerType.POST))
                .then(redisTemplate.opsForValue().delete(POST_CACHE_PREFIX + postId).then())
                .then(postDetailsRepository.deleteById(postId))
                .doOnError(error -> log.error(
                        "|PostMediaModerationOrchestrator|cleanupFailedPost|postId={}|error={}",
                        postId, error.getMessage()))
                .onErrorResume(error -> postDetailsRepository.deleteById(postId).then());
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

        static PostScanOutcome failed(PostMediaScanItem item, String reason) {
            return new PostScanOutcome(item, false, reason);
        }

        boolean processingFailure() {
            return "PROCESSING_ERROR".equals(reason);
        }
    }
}