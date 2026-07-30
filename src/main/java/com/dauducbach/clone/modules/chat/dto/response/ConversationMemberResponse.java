package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.MemberRole;

public record ConversationMemberResponse(
        String userId,
        String displayName,
        String username,
        String fullName,
        String nickname,
        String avatarUrl,
        MemberRole role
) {
}
