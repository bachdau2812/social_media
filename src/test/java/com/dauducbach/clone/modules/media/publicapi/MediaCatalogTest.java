package com.dauducbach.clone.modules.media.publicapi;

import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.service.MediaCatalogService;
import com.dauducbach.clone.modules.media.service.MediaService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MediaCatalogTest {
    @Mock
    private MediaService mediaService;

    @Test
    void returnsStableAssetSnapshotWithoutPersistenceEntity() {
        Instant createdAt = Instant.parse("2026-10-01T12:00:00Z");
        when(mediaService.getByPublicId("posts/asset-1")).thenReturn(Mono.just(Media.builder()
                .assetId("asset-1")
                .publicId("posts/asset-1")
                .width(1280)
                .height(720)
                .mediaFormat("jpg")
                .resourceType("image")
                .bytes(42)
                .url("http://media.test/asset-1")
                .secureUrl("https://media.test/asset-1")
                .ownerId("post-1")
                .ownerType(OwnerType.POST)
                .version("3")
                .versionId("v3")
                .displayName("Cover")
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .build()));

        StepVerifier.create(new MediaCatalogService(mediaService).findByPublicId("posts/asset-1"))
                .expectNext(new MediaAssetView(
                        "asset-1", "posts/asset-1", 1280, 720, "jpg", "image", 42,
                        "http://media.test/asset-1", "https://media.test/asset-1", "post-1",
                        OwnerType.POST, "3", "v3", "Cover", createdAt, createdAt))
                .verifyComplete();
    }
}
