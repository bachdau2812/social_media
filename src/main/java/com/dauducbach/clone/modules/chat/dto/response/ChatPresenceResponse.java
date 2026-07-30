package com.dauducbach.clone.modules.chat.dto.response;

import java.time.Instant;

public record ChatPresenceResponse(String userId, boolean online, Instant lastActiveAt) {
}