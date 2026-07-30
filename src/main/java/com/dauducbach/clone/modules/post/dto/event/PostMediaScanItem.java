package com.dauducbach.clone.modules.post.dto.event;

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
public class PostMediaScanItem {
    Integer orderNumber;
    String secureUrl;
    String publicId;
    String resourceType;
    String caption;
    String musicId;
    Long musicStart;
    Long musicEnd;
}