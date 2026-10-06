package com.dauducbach.clone.modules.chat.dto.request;

import com.dauducbach.clone.modules.chat.constant.ReactionType;
import jakarta.validation.constraints.NotNull;

public record SetMessageReactionRequest(@NotNull ReactionType reaction) {}
