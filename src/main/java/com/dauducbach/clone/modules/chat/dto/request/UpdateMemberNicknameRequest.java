package com.dauducbach.clone.modules.chat.dto.request;

import jakarta.validation.constraints.Size;

public record UpdateMemberNicknameRequest(
        @Size(max = 100, message = "nickname must not exceed 100 characters") String nickname
) {
}
