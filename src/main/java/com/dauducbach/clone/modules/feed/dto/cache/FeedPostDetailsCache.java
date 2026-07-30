package com.dauducbach.clone.modules.feed.dto.cache;

import com.dauducbach.clone.modules.feed.dto.response.FeedMediaResponse;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.time.Instant;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class FeedPostDetailsCache {
    int schemaVersion;
    String postId;
    String userId;
    String content;
    String hashtag;
    String mediaRatio;
    Instant createdAt;
    Instant updatedAt;
    String validateStatus;
    String authorUsername;
    String authorFullName;
    String authorAvatarUrl;
    List<String> hashtags;
    List<FeedMediaResponse> media;
    String musicId;
    Long musicStart;
    Long musicEnd;
    PostMusicResponse music;
    List<PostItemResponse> items;
}
