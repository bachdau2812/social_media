package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface PostDetailsRepository extends ReactiveCrudRepository<PostDetails, String> {
    @Query("SELECT user_id FROM post_details WHERE post_id = :postId")
    Mono<String> findUserIdByPostId(String postId);

    @Query("SELECT * FROM post_details WHERE user_id = :userId ORDER BY post_id DESC LIMIT :limit OFFSET :offset")
    Flux<PostDetails> findByUserId(String userId, int limit, int offset);

    Flux<PostDetails> findAllByUserId(String userId);

    Mono<Void> deleteByUserId(String userId);
}

