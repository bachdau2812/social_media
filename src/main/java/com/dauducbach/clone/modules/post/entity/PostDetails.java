package com.dauducbach.clone.modules.post.entity;

import com.dauducbach.clone.utils.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Table("post_details")
public class PostDetails {
    @Id
    String postId;
    String userId;
    String content;

    /**
     * Luu hashtag duoi dang JSON string trong database.
     */
    String hashtag;
    Instant createdAt;
    Instant updatedAt;
    String validateStatus;
    String musicId;
    Long musicStart;
    Long musicEnd;
    String mediaRatio;

    public List<String> getHashtagList() {
        if (hashtag == null || hashtag.trim().isEmpty()) {
            return new ArrayList<>();
        }
        try {
            return GsonUtils.getGson().fromJson(hashtag, new TypeToken<List<String>>() {}.getType());
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    public void setHashtagList(List<String> hashtags) {
        if (hashtags == null || hashtags.isEmpty()) {
            this.hashtag = "[]";
        } else {
            try {
                this.hashtag = GsonUtils.getGson().toJson(hashtags);
            } catch (Exception e) {
                this.hashtag = "[]";
            }
        }
    }
}
