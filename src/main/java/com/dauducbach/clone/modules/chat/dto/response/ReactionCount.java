package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.ReactionType;

public record ReactionCount(ReactionType type, long count) {}
