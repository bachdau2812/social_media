package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MessageType;

import java.time.Instant;

public record ConversationResponse(
        String id, ConversationType type, boolean isDissolved, String title, String avatarUrl,
        long lastMessageSeq, String lastMessageId, Instant lastMessageAt,
        String lastMessageSenderId, MessageType lastMessageType, String lastMessagePreview,
        MemberRole currentUserRole, long unreadCount,
        long recipientDeliveredSeq, long recipientReadSeq, Instant createdAt) {
}
