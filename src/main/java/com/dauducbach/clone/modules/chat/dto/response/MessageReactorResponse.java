package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.ReactionType;
import java.time.Instant;

public record MessageReactorResponse(String userId, String displayName, String avatarUrl,
                                     ReactionType reaction, Instant reactedAt) {}
