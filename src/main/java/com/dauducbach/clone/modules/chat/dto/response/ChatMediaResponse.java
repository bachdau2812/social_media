package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.MessageType;

import java.time.Instant;

public record ChatMediaResponse(
        String messageId,
        long messageSeq,
        MessageType messageType,
        MediaMetadataResponse media,
        Instant createdAt
) {
}