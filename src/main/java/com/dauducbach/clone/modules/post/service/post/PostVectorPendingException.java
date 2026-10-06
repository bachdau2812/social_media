package com.dauducbach.clone.modules.post.service.post;
/** Retryable: do not ACK an interaction while its current vector needs rebuild or repair. */
public class PostVectorPendingException extends RuntimeException {
    public PostVectorPendingException(String postId, String reason) { super("Post vector pending: " + postId + " (" + reason + ")"); }
}
