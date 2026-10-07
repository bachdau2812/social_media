package com.dauducbach.clone.modules.post.publishing;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.dto.request.PostCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostItemCreateRequest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostPublicationPolicyTest {
    @Test
    void requiresContentOrMedia() {
        assertThatThrownBy(() -> PostPublicationPolicy.validateCreateRequest(
                PostCreateRequest.builder().userId("user-1").content("  ").items(List.of()).build()))
                .isInstanceOf(AppException.class)
                .extracting(error -> ((AppException) error).getErrorCode())
                .isEqualTo(ErrorCode.POST_CONTENT_INVALID);
    }

    @Test
    void acceptsRatioConventionAndSanitizesContent() {
        var request = PostCreateRequest.builder()
                .userId("user-1")
                .content("<script>alert(1)</script><strong>hello</strong>")
                .mediaRatio("16:9")
                .items(List.of())
                .build();

        PostPublicationPolicy.validateCreateRequest(request);

        assertThat(PostPublicationPolicy.sanitizeContent(request.getContent(), false)).isEqualTo("<strong>hello</strong>");
        assertThat(PostPublicationPolicy.normalizeRatio(request.getMediaRatio())).isEqualTo("16:9");
    }

    @Test
    void ignoresPerItemMusicForVideoAndKeepsImageMusicForModeration() {
        var request = PostCreateRequest.builder()
                .userId("user-1")
                .items(List.of(
                        PostItemCreateRequest.builder().secureUrl("https://cdn.example/video.mp4")
                                .publicId("video-1").musicId("track-video").musicStart(0L).musicEnd(10L).build(),
                        PostItemCreateRequest.builder().secureUrl("https://cdn.example/image.jpg")
                                .publicId("image-1").musicId("track-image").musicStart(2L).musicEnd(12L).build()))
                .build();

        PostPublicationPolicy.validateCreateRequest(request);

        var scanItems = PostPublicationPolicy.buildScanItems(request);
        assertThat(scanItems).extracting("musicId").containsExactly(null, "track-image");
        assertThat(scanItems).extracting("orderNumber").containsExactly(1, 2);
    }
}
