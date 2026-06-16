package com.dauducbach.clone.utils;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GsonUtilsTest {
    @Test
    void fromStringParsesJsonObjectString() {
        JsonObject payload = GsonUtils.fromString("{\"postId\":\"post-1\",\"userId\":\"user-1\"}");

        assertThat(payload.get("postId").getAsString()).isEqualTo("post-1");
        assertThat(payload.get("userId").getAsString()).isEqualTo("user-1");
    }

    @Test
    void fromStringParsesDoubleEncodedJsonObjectString() {
        JsonObject payload = GsonUtils.fromString("\"{\\\"commentId\\\":\\\"comment-1\\\",\\\"postId\\\":\\\"post-1\\\"}\"");

        assertThat(payload.get("commentId").getAsString()).isEqualTo("comment-1");
        assertThat(payload.get("postId").getAsString()).isEqualTo("post-1");
    }
}
