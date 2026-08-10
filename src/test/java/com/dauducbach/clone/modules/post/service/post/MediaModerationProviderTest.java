package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.utils.MediaScanUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MediaModerationProviderTest {
    private MediaScanUtils scanner;
    private MediaModerationProvider provider;

    @BeforeEach
    void setUp() {
        scanner = mock(MediaScanUtils.class);
        MediaPolicyProperties policy = new MediaPolicyProperties();
        policy.setImage(DataSize.ofMegabytes(100));
        policy.setVideo(DataSize.ofMegabytes(100));
        policy.setAudio(DataSize.ofMegabytes(50));
        provider = new MediaModerationProvider(scanner, policy);
    }

    @Test
    void explicitVideoBypassesDownloadAndExternalScan() {
        StepVerifier.create(provider.scan(
                        "https://res.cloudinary.com/demo/video/upload/v1/movie.mp4",
                        "movie",
                        "VIDEO"))
                .expectNext(MediaModerationProvider.Decision.APPROVED)
                .verifyComplete();

        verify(scanner, never()).scanMedia(
                "https://res.cloudinary.com/demo/video/upload/v1/movie.mp4",
                "movie");
    }

    @Test
    void videoUrlBypassesScanWhenTypeIsMissing() {
        StepVerifier.create(provider.scan("https://cdn.example/movie.webm?x=1", "movie", null))
                .expectNext(MediaModerationProvider.Decision.APPROVED)
                .verifyComplete();

        verify(scanner, never()).scanMedia("https://cdn.example/movie.webm?x=1", "movie");
    }

    @Test
    void imagesAndUnknownMediaStillUseScanner() {
        when(scanner.scanMedia("https://cdn.example/image.jpg", "image"))
                .thenReturn(Mono.just(MediaScanUtils.ScanResult.approved()));
        when(scanner.scanMedia("https://cdn.example/no-extension", "unknown"))
                .thenReturn(Mono.just(MediaScanUtils.ScanResult.rejected()));

        StepVerifier.create(provider.scan("https://cdn.example/image.jpg", "image", "image"))
                .expectNext(MediaModerationProvider.Decision.APPROVED)
                .verifyComplete();
        StepVerifier.create(provider.scan("https://cdn.example/no-extension", "unknown", null))
                .expectNext(MediaModerationProvider.Decision.REJECTED)
                .verifyComplete();
    }

    @Test
    void appliesOneHundredMegabyteLimitToImagesAndVideos() {
        int exactLimit = 100 * 1024 * 1024;

        assertThat(provider.isAllowedAsset(Media.builder().resourceType("image").bytes(exactLimit).build())).isTrue();
        assertThat(provider.isAllowedAsset(Media.builder().resourceType("video").bytes(exactLimit).build())).isTrue();
        assertThat(provider.isAllowedAsset(Media.builder().resourceType("image").bytes(exactLimit + 1).build())).isFalse();
        assertThat(provider.isAllowedAsset(Media.builder().resourceType("video").bytes(exactLimit + 1).build())).isFalse();
    }
}
