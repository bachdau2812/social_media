package com.dauducbach.clone.modules.post.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

public class PostCreateRequest {
    String userId;
    String content;
    List<String> hashtags;
    String mediaRatio;
    String musicId;
    Long musicStart;
    Long musicEnd;
    List<PostItemCreateRequest> items;
}
