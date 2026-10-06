package com.dauducbach.clone.modules.post.dto.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public record PostInteractionRequest(
        String postId,
        @JsonDeserialize(using = PostInteractionRequestDeserializers.StrictBoolean.class) Boolean isClick,
        @JsonDeserialize(using = PostInteractionRequestDeserializers.StrictViewTime.class) Integer viewTime,
        String eventId,
        String impressionId
) {}
