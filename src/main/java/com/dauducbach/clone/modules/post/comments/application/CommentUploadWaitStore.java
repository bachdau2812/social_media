package com.dauducbach.clone.modules.post.comments.application;

import reactor.core.publisher.Mono;

public interface CommentUploadWaitStore {
    Mono<Void> markWaitingForUpload(String commentId, String userId);
}
