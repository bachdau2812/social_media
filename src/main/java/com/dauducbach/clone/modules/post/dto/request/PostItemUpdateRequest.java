package com.dauducbach.clone.modules.post.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder
public class PostItemUpdateRequest {
    String itemId;
    Integer orderNumber;
    String secureUrl;
    String publicId;
    String resourceType;
    String caption;
    String musicId;
    Long musicStart;
    Long musicEnd;
}
