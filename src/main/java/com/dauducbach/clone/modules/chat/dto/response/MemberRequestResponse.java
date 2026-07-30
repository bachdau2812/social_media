package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.MemberRequestStatus;

import java.time.Instant;

public record MemberRequestResponse(
        String id,
        String conversationId,
        MemberRequestStatus status,
        ChatUserSummaryResponse requester,
        ChatUserSummaryResponse target,
        Instant createdAt
) {
}