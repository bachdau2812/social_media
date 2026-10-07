package com.dauducbach.clone.modules.post.publicapi;

import com.dauducbach.clone.commons.response.PageResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

public interface CommentPresentationQuery {
    Mono<PageResponse<CommentSnapshot>> getRootComments(String postId, String viewerId, int page, int size);

    Flux<CommentSnapshot> getReplies(String parentId, String viewerId, int page, int size);

    record CommentSnapshot(
            String id,
            String postId,
            String userId,
            String parentId,
            String content,
            String commentType,
            String mediaUrl,
            Instant timestamp,
            long replyCount,
            boolean hasLiked,
            String username,
            String fullName,
            String avatarUrl) { }
}
