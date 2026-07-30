package com.dauducbach.clone.modules.user.dto.request;

import jakarta.validation.constraints.NotBlank;

public record StoryCreateRequest(
        @NotBlank(message = "userId is required")
        String userId,

        @NotBlank(message = "mediaUrl is required")
        String mediaUrl,

        String musicId,

        String musicUrl,

        Long musicStart,

        Long musicEnd,

        String publicationId,

        Integer publicationOrder,

        Integer publicationItemCount
) {
    public StoryCreateRequest(String userId, String mediaUrl, String musicId, String musicUrl, Long musicStart, Long musicEnd) {
        this(userId, mediaUrl, musicId, musicUrl, musicStart, musicEnd, null, null, null);
    }

    public StoryCreateRequest(String userId, String mediaUrl, String musicUrl, Long musicStart, Long musicEnd) {
        this(userId, mediaUrl, null, musicUrl, musicStart, musicEnd, null, null, null);
    }
}
