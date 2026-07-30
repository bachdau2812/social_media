package com.dauducbach.clone.modules.post.dto.response;

public record PostItemResponse(
        String id,
        Integer orderNumber,
        String caption,
        PostMediaResponse media,
        PostMusicResponse music
) {
}
