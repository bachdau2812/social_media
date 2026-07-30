package com.dauducbach.clone.modules.chat.dto.request;

import com.dauducbach.clone.modules.chat.constant.MemberRole;
import jakarta.validation.constraints.NotNull;

public record ChangeMemberRoleRequest(@NotNull MemberRole role) {
}
