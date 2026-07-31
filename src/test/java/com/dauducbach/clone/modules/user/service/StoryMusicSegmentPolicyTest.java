package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StoryMusicSegmentPolicyTest {

    private final StoryMusicSegmentPolicy policy = new StoryMusicSegmentPolicy();

    @Test
    void defaultsImageWithoutMusicToFiveSeconds() {
        assertThat(policy.durationSeconds("IMAGE", null, null)).isEqualTo(5L);
    }

    @Test
    void usesSelectedMusicLengthForImage() {
        assertThat(policy.durationSeconds("IMAGE", 12L, 57L)).isEqualTo(45L);
    }

    @Test
    void leavesVideoDurationToMediaMetadata() {
        assertThat(policy.durationSeconds("VIDEO", 0L, 30L)).isNull();
    }

    @Test
    void acceptsSegmentsBetweenOneAndSixtySeconds() {
        policy.validate("music-1", 4L, 5L);
        policy.validate("music-1", 4L, 64L);
    }

    @ParameterizedTest
    @CsvSource({"0,0", "10,9", "0,61", "-1,20"})
    void rejectsInvalidSegments(long start, long end) {
        assertThatThrownBy(() -> policy.validate("music-1", start, end))
                .isInstanceOf(AppException.class);
    }

    @Test
    void rejectsPartialOrUnownedMusicSegments() {
        assertThatThrownBy(() -> policy.validate("music-1", 0L, null))
                .isInstanceOf(AppException.class);
        assertThatThrownBy(() -> policy.validate(null, 0L, 30L))
                .isInstanceOf(AppException.class);
    }
}
