package com.dauducbach.clone.modules.chat.dto.response;

public record MemberNicknameResponse(
        String conversationId,
        String userId,
        String nickname
) {
}
