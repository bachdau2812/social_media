package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.PostRepost;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface PostRepostRepository extends ReactiveCrudRepository<PostRepost, String> {
    Mono<PostRepost> findByActorIdAndPostId(String actorId, String postId);

    Mono<Boolean> existsByActorIdAndPostId(String actorId, String postId);

    Mono<Long> countByPostId(String postId);

    Mono<Long> countByActorId(String actorId);

    Mono<Void> deleteByActorIdAndPostId(String actorId, String postId);

    @Query("SELECT actor_id FROM post_reposts WHERE post_id = :postId ORDER BY created_at DESC LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}")
    Flux<String> findActorIdsByPostId(String postId, Pageable pageable);

    @Query("SELECT post_id FROM post_reposts WHERE actor_id = :actorId ORDER BY created_at DESC LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}")
    Flux<String> findPostIdsByActorId(String actorId, Pageable pageable);
}