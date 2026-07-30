package com.dauducbach.clone.modules.media.constant;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MediaDisplayTypeTest {
    @Test
    void searchThumbnailUsesCompactAutoGravityFillTransformation() {
        assertThat(MediaDisplayType.SEARCH_THUMBNAIL.transformations())
                .containsExactly("c_fill", "g_auto", "w_480", "h_360", "q_auto:good", "f_auto");
    }
}
