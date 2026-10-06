package com.dauducbach.clone.modules.post.dto.response;

public record PostInteractionAcceptedResponse(String eventId, int computedScore, boolean duplicate) {}
