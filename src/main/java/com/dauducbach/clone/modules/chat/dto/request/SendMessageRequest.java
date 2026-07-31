package com.dauducbach.clone.modules.chat.dto.request;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public record SendMessageRequest(
        @NotBlank String clientMessageId,
        @NotNull MessageType messageType,
        String content,
        @Valid MediaMetadataRequest metadata,
        Long replyToSeq,
        String recipientId,
        List<String> recipientIds,
        @Valid StoryContextRequest storyContext) {
}
