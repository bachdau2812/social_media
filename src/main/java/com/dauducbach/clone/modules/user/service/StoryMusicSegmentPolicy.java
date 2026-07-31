package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import org.springframework.stereotype.Component;

@Component
public class StoryMusicSegmentPolicy {

    public static final long DEFAULT_IMAGE_SECONDS = 5L;
    public static final long MAX_MUSIC_SECONDS = 60L;

    public void validate(String musicReference, Long musicStart, Long musicEnd) {
        if (musicStart == null && musicEnd == null) {
            return;
        }
        if (musicReference == null || musicReference.isBlank()
                || musicStart == null
                || musicEnd == null
                || musicStart < 0
                || musicEnd <= musicStart
                || musicEnd - musicStart > MAX_MUSIC_SECONDS) {
            throw new AppException(
                    ErrorCode.PROFILE_MEDIA_INVALID,
                    "Story music segment must be between 1 and 60 seconds");
        }
    }

    public Long durationSeconds(String mediaType, Long musicStart, Long musicEnd) {
        if ("VIDEO".equalsIgnoreCase(mediaType)) {
            return null;
        }
        if (musicStart != null && musicEnd != null && musicEnd > musicStart) {
            return musicEnd - musicStart;
        }
        return DEFAULT_IMAGE_SECONDS;
    }
}
