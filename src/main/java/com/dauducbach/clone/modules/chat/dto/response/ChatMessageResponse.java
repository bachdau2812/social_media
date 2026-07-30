package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.MessageType;

import java.time.Instant;

public record ChatMessageResponse(
        String id, String conversationId, long messageSeq, String clientMessageId,
        String senderId, String senderDisplayName, String senderAvatarUrl,
        MessageType messageType, String content,
        MediaMetadataResponse metadata, Long replyToSeq, ReplyMessageResponse reply,
        Instant createdAt, Instant editedAt, boolean deleted) {
}
