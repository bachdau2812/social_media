package com.dauducbach.clone.modules.chat.dto.response;

public record MemberMutationResponse(String conversationId, String targetUserId, String result, String requestId) {
}
