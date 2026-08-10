package com.dauducbach.clone.modules.media.controller;

import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class MediaPolicyControllerTest {

    @Test
    void returnsTheAuthoritativeUploadLimits() {
        MediaPolicyProperties properties = new MediaPolicyProperties();
        properties.setImage(DataSize.ofMegabytes(100));
        properties.setVideo(DataSize.ofMegabytes(100));
        properties.setAudio(DataSize.ofMegabytes(50));
        MediaPolicyController controller = new MediaPolicyController(properties);

        StepVerifier.create(controller.getUploadPolicy())
                .assertNext(response -> {
                    assertThat(response.getResult().imageMaxBytes()).isEqualTo(100L * 1024 * 1024);
                    assertThat(response.getResult().videoMaxBytes()).isEqualTo(100L * 1024 * 1024);
                    assertThat(response.getResult().audioMaxBytes()).isEqualTo(50L * 1024 * 1024);
                })
                .verifyComplete();
    }
}
