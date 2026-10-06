package com.dauducbach.clone.modules.chat.dto.response;
import java.time.Instant;
public record PinnedMessageResponse(ChatMessageResponse message, String pinnedBy, Instant pinnedAt) {}
