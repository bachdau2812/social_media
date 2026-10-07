package com.dauducbach.clone.modules.media.infrastructure.cloudinary;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CloudinaryUrlTransformServiceTest {
    private final CloudinaryUrlTransformService transforms = new CloudinaryUrlTransformService();

    @Test
    void keepsDeliveryAndPlaybackUrlConventionsAtTheAdapterBoundary() {
        String source = "https://res.cloudinary.com/demo/video/upload/v42/story.mp4";

        assertThat(transforms.transformDeliveryUrl(source, MediaDisplayType.STORY))
                .isEqualTo("https://res.cloudinary.com/demo/video/upload/c_fill,g_auto,w_1080,h_1920,q_auto:good,f_auto/v42/story.mp4");
        assertThat(transforms.storyVideoStill(source, 1500))
                .isEqualTo("https://res.cloudinary.com/demo/video/upload/so_1.5,f_jpg,q_auto/v42/story.mp4");
        assertThat(transforms.transformMusicUrl(
                        "https://res.cloudinary.com/demo/video/upload/v42/song.mp3", 10L, 25L))
                .isEqualTo("https://res.cloudinary.com/demo/video/upload/so_10,du_15/v42/song.mp3");
    }
}
