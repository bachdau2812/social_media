package com.dauducbach.clone.modules.post.dto.response;

public record RepostToggleResponse(
        String postId,
        boolean reposted,
        String repostId,
        long repostCount
) {
}