package com.dauducbach.clone.modules.chat.dto.response;

import java.util.List;

public record ConversationDetailsResponse(
        String conversationId,
        boolean notificationsMuted,
        String createdBy,
        boolean canManageGroup,
        boolean isDissolved,
        List<ConversationMemberResponse> members
) {
}