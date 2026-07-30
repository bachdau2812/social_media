package com.dauducbach.clone.modules.post.dto.request;

import jakarta.validation.constraints.NotBlank;

public record SavePostRequest(@NotBlank String postId) {
}
