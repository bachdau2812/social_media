package com.dauducbach.clone.modules.post.comments.application;

import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.modules.post.entity.Comment;
import reactor.core.publisher.Mono;

import java.util.List;

public interface CommentCommitter {
    Mono<Comment> saveApproved(Comment comment);

    Mono<Comment> savePendingMedia(Comment comment, List<MediaUploadRequest> media);
}
