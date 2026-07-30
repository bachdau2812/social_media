package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.MessageType;

public record ReplyMessageResponse(
        long messageSeq,
        String senderId,
        String senderDisplayName,
        MessageType messageType,
        String content,
        MediaMetadataResponse metadata,
        boolean deleted) {
}
