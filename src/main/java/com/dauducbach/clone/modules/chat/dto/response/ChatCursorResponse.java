package com.dauducbach.clone.modules.chat.dto.response;

public record ChatCursorResponse(String conversationId, long deliveredSeq, long readSeq) {
}
