package com.dauducbach.clone.modules.feed.dto.cache;

import com.dauducbach.clone.modules.feed.dto.response.FeedMediaResponse;
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
    String postId;
    String userId;
    String content;
    String hashtag;
    Instant createdAt;
    Instant updatedAt;
    String validateStatus;
    String authorUsername;
    List<String> hashtags;
    List<FeedMediaResponse> media;
    long likeCount;
    long commentCount;
}
