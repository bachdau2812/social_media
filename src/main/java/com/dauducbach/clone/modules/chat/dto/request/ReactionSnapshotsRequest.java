package com.dauducbach.clone.modules.chat.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import java.util.List;

public record ReactionSnapshotsRequest(@NotNull @Size(max = 100) List<@NotBlank String> messageIds) {}
