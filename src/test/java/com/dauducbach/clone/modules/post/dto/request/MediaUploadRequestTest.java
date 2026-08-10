package com.dauducbach.clone.modules.post.dto.request;

import com.dauducbach.clone.utils.GsonUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MediaUploadRequestTest {

    @Test
    void serializesResourceTypeForModerationEvents() {
        MediaUploadRequest request = MediaUploadRequest.builder()
                .secureUrl("https://res.cloudinary.com/demo/video/upload/v1/comment.mp4")
                .publicId("comments/comment-video")
                .resourceType("video")
                .build();

        assertThat(GsonUtils.getGson().toJsonTree(request).getAsJsonObject()
                .get("resourceType").getAsString()).isEqualTo("video");
    }
}
