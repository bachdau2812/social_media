package com.dauducbach.clone.modules.chat.dto.request;
import jakarta.validation.constraints.*;
import java.util.List;
public record MessageStatesRequest(@NotNull @Size(max=100) List<@NotBlank String> messageIds) {}
