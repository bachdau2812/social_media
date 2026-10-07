package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.cloudinary.Cloudinary;
import com.cloudinary.Uploader;
import com.dauducbach.clone.modules.media.infrastructure.cloudinary.CloudinaryMediaService;
import com.dauducbach.clone.modules.media.infrastructure.cloudinary.CloudinaryUrlTransformService;
import com.dauducbach.clone.modules.media.infrastructure.cloudinary.MediaAssetCleanupService;
import com.dauducbach.clone.modules.media.service.MediaAssetsService;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repository.PostItemRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class PostMediaModerationOrchestratorTest {
    private final PostDetailsRepository posts = mock(PostDetailsRepository.class);
    private final MediaAssets assets = mock(MediaAssets.class);
    private final PostItemRepository items = mock(PostItemRepository.class);
    private final R2dbcEntityTemplate sql = mock(R2dbcEntityTemplate.class, RETURNS_DEEP_STUBS);
    private final ReactiveRedisTemplate<String, String> redis = mock(ReactiveRedisTemplate.class);
    private final ReactiveValueOperations<String, String> values = mock(ReactiveValueOperations.class);
    private final PostSseService sse = mock(PostSseService.class);
    private final KafkaSender<String, String> kafka = mock(KafkaSender.class);
    private final MediaModerationProvider moderation = mock(MediaModerationProvider.class);
    private final PostVectorService vectors = mock(PostVectorService.class);
    private final PostDetails post = PostDetails.builder().postId("post-1").userId("user-1").build();
    private final List<PostItem> inserted = new ArrayList<>();
    private final PostMediaModerationOrchestrator orchestrator = new PostMediaModerationOrchestrator(
            posts, assets, items, redis, sse, kafka, moderation, vectors, sql);

    PostMediaModerationOrchestratorTest() {
        when(posts.claimPendingMediaScan(anyString(), anyString(), anyString(), any())).thenReturn(Mono.just(1));
        when(posts.releaseMediaScanClaim(anyString(), anyString(), anyString(), any())).thenReturn(Mono.just(1));
        when(items.deleteByPostId("post-1")).thenReturn(Mono.empty());
        // Reproduce repository.save treating assigned UUIDs as existing rows.
        when(items.save(any(PostItem.class))).thenReturn(Mono.error(new IllegalStateException(
                "Failed to update table [post_items]; Row with Id does not exist")));
        when(sql.insert(PostItem.class).using(any(PostItem.class))).thenAnswer(i -> Mono.fromSupplier(() -> {
            PostItem item = i.getArgument(0); inserted.add(item); return item;
        }));
        when(redis.opsForValue()).thenReturn(values);
        when(values.delete(anyString())).thenReturn(Mono.just(true));
        when(posts.findById("post-1")).thenReturn(Mono.just(post));
        when(posts.save(any(PostDetails.class))).thenAnswer(i -> Mono.just(i.getArgument(0)));
        when(assets.deleteAsset(anyString())).thenReturn(Mono.empty());
        when(assets.deleteAssets(anyCollection())).thenReturn(Mono.empty());
        when(assets.deleteAssetsForOwner("post-1", OwnerType.POST)).thenReturn(Mono.empty());
        when(posts.deleteById("post-1")).thenReturn(Mono.empty());
        when(vectors.deletePost("post-1", "user-1")).thenReturn(Mono.empty());
        when(vectors.rebuild("post-1")).thenReturn(Mono.empty());
        when(sse.sendToUser(eq("user-1"), eq("post_upload"), anyString())).thenReturn(Mono.empty());
        when(kafka.send(any())).thenReturn(Flux.just(mock(SenderResult.class)));
    }

    @Test void approvedImageInsertsNewUuidItemBeforeApprovingPost() {
        approve();
        StepVerifier.create(orchestrator.process("post-1", "user-1", List.of(item("posts/image", 1)))).verifyComplete();
        assertThat(inserted).singleElement().satisfies(saved -> {
            assertThat(saved.getId()).isNotBlank();
            assertThat(saved.getPostId()).isEqualTo("post-1");
            assertThat(saved.getMediaId()).isEqualTo("asset-1");
            assertThat(saved.getOrderNumber()).isEqualTo(1);
        });
        assertThat(post.getValidateStatus()).isEqualTo("APPROVED");
        verify(items, never()).save(any());
        verify(assets, never()).deleteAssets(anyCollection());
    }

    @Test void scannerFailureDeletesAllInputAssetsIncludingUnprocessedImages() {
        RuntimeException failure = new IllegalStateException("scanner unavailable");
        when(moderation.scan(anyString(), eq("posts/image"), eq("image"))).thenReturn(Mono.error(failure));
        StepVerifier.create(orchestrator.process("post-1", "user-1",
                List.of(item("posts/image", 1), item("posts/unprocessed", 2))))
                .expectErrorMatches(error -> error == failure).verify();
        verify(assets).deleteAssets(List.of("posts/image", "posts/unprocessed"));
        verify(assets).deleteAssetsForOwner("post-1", OwnerType.POST);
        verify(posts).deleteById("post-1");
        verify(values).delete("post:details:v3:post-1");
        verify(vectors).deletePost("post-1", "user-1");
        verify(sse).sendToUser(eq("user-1"), eq("post_upload"), contains("FAILED"));
        verify(posts, never()).releaseMediaScanClaim(anyString(), anyString(), anyString(), any());
    }

    @Test void itemInsertFailureCleansRegisteredAndUnprocessedMedia() {
        approve();
        RuntimeException failure = new IllegalStateException("item insert failed");
        when(sql.insert(PostItem.class).using(any(PostItem.class))).thenReturn(Mono.error(failure));
        StepVerifier.create(orchestrator.process("post-1", "user-1",
                List.of(item("posts/image", 1), item("posts/unprocessed", 2))))
                .expectErrorMatches(error -> error == failure).verify();
        verify(assets).deleteAssets(List.of("posts/image", "posts/unprocessed"));
        verify(assets).deleteAssetsForOwner("post-1", OwnerType.POST);
        verify(posts).deleteById("post-1");
        verify(kafka, never()).send(any());
    }

    @Test void cleanupFailureDoesNotSkipOtherCleanupOrReplaceOriginalFailure() {
        RuntimeException failure = new IllegalStateException("scanner unavailable");
        when(moderation.scan(anyString(), anyString(), anyString())).thenReturn(Mono.error(failure));
        when(assets.deleteAssets(anyCollection())).thenReturn(Mono.error(new IllegalStateException("Cloudinary unavailable")));
        StepVerifier.create(orchestrator.process("post-1", "user-1", List.of(item("posts/image", 1))))
                .expectErrorMatches(error -> error == failure).verify();
        verify(assets).deleteAssetsForOwner("post-1", OwnerType.POST);
        verify(posts).deleteById("post-1");
        verify(values).delete("post:details:v3:post-1");
    }

    @Test void duplicateEventCannotCleanAnotherWorkersPost() {
        when(posts.claimPendingMediaScan(anyString(), anyString(), anyString(), any())).thenReturn(Mono.just(0));
        StepVerifier.create(orchestrator.process("post-1", "user-1", List.of(item("posts/image", 1)))).verifyComplete();
        verifyNoInteractions(assets, moderation);
        verify(posts, never()).deleteById(anyString());
    }

    @Test void missingPostStillDeletesUploadedMedia() {
        when(posts.findById("post-1")).thenReturn(Mono.empty());
        StepVerifier.create(orchestrator.discardFailedPost("post-1", List.of(item("posts/image", 1)))).verifyComplete();
        verify(assets).deleteAssets(List.of("posts/image"));
        verify(assets).deleteAssetsForOwner("post-1", OwnerType.POST);
        verify(posts).deleteById("post-1");
        verify(vectors).rebuild("post-1");
    }

    @Test void laterScanFailureAlsoCleansPreviouslyInsertedMedia() {
        approve();
        RuntimeException failure = new IllegalStateException("second image scan failed");
        when(moderation.scan(anyString(), eq("posts/second"), anyString())).thenReturn(Mono.error(failure));
        StepVerifier.create(orchestrator.process("post-1", "user-1",
                List.of(item("posts/image", 1), item("posts/second", 2))))
                .expectErrorMatches(error -> error == failure).verify();
        assertThat(inserted).hasSize(1);
        verify(assets).deleteAssets(List.of("posts/image", "posts/second"));
        verify(items, times(2)).deleteByPostId("post-1");
        verify(posts).deleteById("post-1");
    }

    @Test void partialNsfwRejectionPublishesApprovedMediaOnly() {
        approve();
        when(moderation.scan(anyString(), eq("posts/rejected"), anyString()))
                .thenReturn(Mono.just(MediaModerationProvider.Decision.REJECTED));
        StepVerifier.create(orchestrator.process("post-1", "user-1",
                List.of(item("posts/rejected", 2), item("posts/image", 1)))).verifyComplete();
        assertThat(inserted).hasSize(1);
        assertThat(post.getValidateStatus()).isEqualTo("APPROVED");
        verify(assets).deleteAsset("posts/rejected");
        verify(assets, never()).deleteAssets(anyCollection());
        verify(posts, never()).deleteById(anyString());
        verify(sse).sendToUser(eq("user-1"), eq("post_upload"), contains("SUCCESSED"));
    }

    @Test void allNsfwRejectedMediaDeletesPostAndReportsFailure() {
        when(moderation.scan(anyString(), anyString(), anyString()))
                .thenReturn(Mono.just(MediaModerationProvider.Decision.REJECTED));
        StepVerifier.create(orchestrator.process("post-1", "user-1", List.of(item("posts/image", 1)))).verifyComplete();
        verify(assets).deleteAsset("posts/image");
        verify(posts).deleteById("post-1");
        verify(kafka, never()).send(any());
        verify(sse).sendToUser(eq("user-1"), eq("post_upload"), contains("FAILED"));
    }

    @Test void kafkaPublicationFailureCannotSendSuccessAndCleansPost() {
        approve();
        RuntimeException failure = new IllegalStateException("Kafka rejected publication");
        SenderResult<String> result = mock(SenderResult.class);
        when(result.exception()).thenReturn(failure);
        when(kafka.<String>send(any())).thenReturn(Flux.just(result));
        StepVerifier.create(orchestrator.process("post-1", "user-1", List.of(item("posts/image", 1))))
                .expectErrorMatches(error -> error == failure).verify();
        verify(assets).deleteAssets(List.of("posts/image"));
        verify(posts).deleteById("post-1");
        verify(sse, never()).sendToUser(anyString(), anyString(), contains("SUCCESSED"));
        verify(sse).sendToUser(eq("user-1"), eq("post_upload"), contains("FAILED"));
    }

    @Test void scannerFailureCallsCloudinaryDestroyThroughExistingMediaAdapter() throws Exception {
        Cloudinary cloudinary = mock(Cloudinary.class);
        Uploader uploader = mock(Uploader.class);
        when(cloudinary.uploader()).thenReturn(uploader);
        when(uploader.destroy(anyString(), anyMap())).thenReturn(Map.of("result", "ok"));
        MediaService mediaService = mock(MediaService.class);
        when(mediaService.deleteByOwnerIdAndOwnerType("post-1", OwnerType.POST)).thenReturn(Mono.empty());
        MediaAssetsService adapter = new MediaAssetsService(mediaService, mock(CloudinaryMediaService.class),
                mock(CloudinaryUrlTransformService.class), new MediaAssetCleanupService(cloudinary));
        PostMediaModerationOrchestrator subject = new PostMediaModerationOrchestrator(
                posts, adapter, items, redis, sse, kafka, moderation, vectors, sql);
        RuntimeException failure = new IllegalStateException("scanner unavailable");
        when(moderation.scan(anyString(), anyString(), anyString())).thenReturn(Mono.error(failure));
        StepVerifier.create(subject.process("post-1", "user-1", List.of(item("posts/image", 1))))
                .expectErrorMatches(error -> error == failure).verify();
        verify(uploader).destroy("posts/image", Map.of());
        verify(uploader).destroy("posts/image", Map.of("resource_type", "video"));
        verify(mediaService).deleteByOwnerIdAndOwnerType("post-1", OwnerType.POST);
    }

    private void approve() {
        MediaAssetView media = new MediaAssetView("asset-1", "posts/image", 800, 600, "jpg", "image", 1024,
                "http://cdn/image", "https://cdn/image", "post-1", OwnerType.POST, "1", "v1", "image", Instant.now(), Instant.now());
        when(moderation.scan(anyString(), eq("posts/image"), eq("image"))).thenReturn(Mono.just(MediaModerationProvider.Decision.APPROVED));
        when(assets.fetchRemoteAsset("posts/image")).thenReturn(Mono.just(media));
        when(moderation.isAllowedAsset(media)).thenReturn(true);
        when(assets.registerFetchedAsset(media, "post-1", OwnerType.POST)).thenReturn(Mono.just(media));
    }

    private PostMediaScanItem item(String publicId, int order) {
        return PostMediaScanItem.builder().orderNumber(order).secureUrl("https://cdn.example.com/image.jpg")
                .publicId(publicId).resourceType("image").build();
    }
}
