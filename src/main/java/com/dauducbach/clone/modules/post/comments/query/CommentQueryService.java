package com.dauducbach.clone.modules.post.comments.query;

import com.dauducbach.clone.modules.post.publicapi.CommentQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class CommentQueryService implements CommentQuery {
    private final CommentReadService commentReadService;

    @Override
    public Mono<CommentSnapshot> findById(String commentId) {
        return commentReadService.getCommentById(commentId)
                .map(comment -> new CommentSnapshot(comment.getId(), comment.getPostId(), comment.getUserId(),
                        comment.getParentId(), comment.getContent()));
    }

    @Override
    public Flux<String> findDistinctCommenterUserIdsByPostId(String postId) {
        return commentReadService.getDistinctCommenterUserIdsByPostId(postId);
    }

    @Override
    public Mono<Long> countByPostId(String postId) {
        return commentReadService.countCommentsByPostId(postId);
    }
}
