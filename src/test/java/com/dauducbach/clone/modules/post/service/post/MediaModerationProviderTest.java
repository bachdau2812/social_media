package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaInspection;
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
    private MediaInspection scanner;
    private MediaModerationProvider provider;

    @BeforeEach
    void setUp() {
        scanner = mock(MediaInspection.class);
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

        verify(scanner, never()).inspect(
                "https://res.cloudinary.com/demo/video/upload/v1/movie.mp4",
                "movie");
    }

    @Test
    void videoUrlBypassesScanWhenTypeIsMissing() {
        StepVerifier.create(provider.scan("https://cdn.example/movie.webm?x=1", "movie", null))
                .expectNext(MediaModerationProvider.Decision.APPROVED)
                .verifyComplete();

        verify(scanner, never()).inspect("https://cdn.example/movie.webm?x=1", "movie");
    }

    @Test
    void imagesAndUnknownMediaStillUseScanner() {
        when(scanner.inspect("https://cdn.example/image.jpg", "image"))
                .thenReturn(Mono.just(new MediaInspection.Result(false)));
        when(scanner.inspect("https://cdn.example/no-extension", "unknown"))
                .thenReturn(Mono.just(new MediaInspection.Result(true)));

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

        assertThat(provider.isAllowedAsset(asset("image", exactLimit))).isTrue();
        assertThat(provider.isAllowedAsset(asset("video", exactLimit))).isTrue();
        assertThat(provider.isAllowedAsset(asset("image", exactLimit + 1))).isFalse();
        assertThat(provider.isAllowedAsset(asset("video", exactLimit + 1))).isFalse();
    }

    private MediaAssetView asset(String type, int bytes) {
        return new MediaAssetView("asset", "public", 0, 0, null, type, bytes,
                null, null, null, null, null, null, null, null, null);
    }
}
