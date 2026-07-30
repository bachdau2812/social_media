package com.dauducbach.clone.modules.media.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudinaryUtilsTest {

    @Test
    void withAudioSegmentInsertsTransformationsBeforeVersion() {
        String transformed = CloudinaryUtils.withAudioSegment(
                "https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3",
                30L,
                45L
        );

        assertThat(transformed)
                .isEqualTo("https://res.cloudinary.com/demo/video/upload/so_30,du_15/v1234567890/musics/song.mp3");
    }

    @Test
    void withTransformationsAppendsToExistingTransformationAndPreservesQuery() {
        String transformed = CloudinaryUtils.withTransformations(
                "https://res.cloudinary.com/demo/image/upload/c_fill,w_300/v1234567890/avatars/me.jpg?token=abc",
                "q_auto",
                "f_auto"
        );

        assertThat(transformed)
                .isEqualTo("https://res.cloudinary.com/demo/image/upload/c_fill,w_300,q_auto,f_auto/v1234567890/avatars/me.jpg?token=abc");
    }

    @Test
    void withAudioSegmentRejectsInvalidRange() {
        assertThatThrownBy(() -> CloudinaryUtils.withAudioSegment(
                "https://res.cloudinary.com/demo/video/upload/v1234567890/musics/song.mp3",
                45L,
                30L
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("musicEnd must be greater than musicStart");
    }
}
