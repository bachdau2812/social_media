package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.Like;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface LikeRepository extends ReactiveCrudRepository<Like, String> {
    Flux<Like> findByTargetId(String targetId);

    Mono<Boolean> existsByActorIdAndTargetIdAndTargetType(String actorId, String targetId, String targetType);

    Mono<Like> findByActorIdAndTargetIdAndTargetType(String actorId, String targetId, String targetType);

    Mono<Long> countByTargetIdAndTargetType(String targetId, String targetType);

    Mono<Long> countByActorIdAndTargetType(String actorId, String targetType);

    @Query("SELECT actor_id FROM likes WHERE target_id = :targetId AND target_type = :targetType ORDER BY timestamp DESC LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}")
    Flux<String> findActorIdsByTargetIdAndTargetType(String targetId, String targetType, Pageable pageable);

    @Query("SELECT target_id FROM likes WHERE actor_id = :actorId AND target_type = :targetType ORDER BY timestamp DESC LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}")
    Flux<String> findTargetIdByActorIdAndTargetType(String actorId, String targetType, Pageable pageable);
}

