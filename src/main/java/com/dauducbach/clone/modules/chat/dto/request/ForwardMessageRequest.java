package com.dauducbach.clone.modules.chat.dto.request;
import jakarta.validation.constraints.NotBlank;
public record ForwardMessageRequest(@NotBlank String sourceConversationId, @NotBlank String sourceMessageId,
                                    @NotBlank String clientMessageId) {}
