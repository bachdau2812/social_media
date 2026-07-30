package com.dauducbach.clone.modules.post.dto.request;

import jakarta.validation.constraints.NotBlank;

public record ArchiveContentRequest(@NotBlank String contentId, @NotBlank String contentType, String thumbnailUrl, String captionPreview) {
}