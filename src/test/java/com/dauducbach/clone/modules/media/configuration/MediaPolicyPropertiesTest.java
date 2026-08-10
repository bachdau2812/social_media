package com.dauducbach.clone.modules.media.configuration;

import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MediaPolicyPropertiesTest {

    @Test
    void exposesConfiguredLimitsAsBytes() {
        MediaPolicyProperties properties = new MediaPolicyProperties();
        properties.setImage(DataSize.ofMegabytes(100));
        properties.setVideo(DataSize.ofMegabytes(100));
        properties.setAudio(DataSize.ofMegabytes(50));
        properties.validate();

        assertThat(properties.imageMaxBytes()).isEqualTo(100L * 1024 * 1024);
        assertThat(properties.videoMaxBytes()).isEqualTo(100L * 1024 * 1024);
        assertThat(properties.audioMaxBytes()).isEqualTo(50L * 1024 * 1024);
    }

    @Test
    void rejectsNonPositiveLimits() {
        MediaPolicyProperties properties = new MediaPolicyProperties();
        properties.setImage(DataSize.ofBytes(0));

        assertThatThrownBy(properties::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("app.media.limits.image");
    }
}
