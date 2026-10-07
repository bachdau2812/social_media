package com.dauducbach.clone.modules.post.comments.infrastructure.persistence;

import com.dauducbach.clone.infrastructure.outbox.InteractionOutbox;
import com.dauducbach.clone.modules.post.comments.application.CommentCommitter;
import com.dauducbach.clone.modules.post.dto.request.MediaUploadRequest;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.repository.CommentRepository;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@RequiredArgsConstructor
public class TransactionalCommentCommitter implements CommentCommitter {
    private final R2dbcEntityTemplate entityTemplate;
    private final InteractionOutbox interactionOutbox;

    @Override
    public Mono<Comment> saveApproved(Comment comment) {
        return interactionOutbox.commit(insert(comment), interactionOutbox::approvedComment);
    }

    @Override
    public Mono<Comment> savePendingMedia(Comment comment, List<MediaUploadRequest> media) {
        return interactionOutbox.commit(insert(comment), saved -> appendMediaScan(saved, media));
    }

    private Mono<Comment> insert(Comment comment) {
        return entityTemplate.insert(Comment.class).using(comment);
    }

    private Mono<Void> appendMediaScan(Comment comment, List<MediaUploadRequest> media) {
        JsonObject payload = new JsonObject();
        payload.addProperty("commentId", comment.getId());
        payload.addProperty("postId", comment.getPostId());
        payload.addProperty("userId", comment.getUserId());
        payload.add("media", GsonUtils.getGson().toJsonTree(media));
        return interactionOutbox.append(
                "COMMENT_SCAN:" + comment.getId(),
                "check_comment_media_event",
                comment.getUserId(),
                payload,
                comment.getTimestamp());
    }
}
