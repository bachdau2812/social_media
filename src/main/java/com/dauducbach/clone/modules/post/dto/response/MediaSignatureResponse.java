package com.dauducbach.clone.modules.post.dto.response;

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
public class MediaSignatureResponse {
    String signature;
    long timestamp;
    String apiKey;
    String folder;
    String uploadPreset;
}

