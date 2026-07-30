package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.PostItem;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface PostItemRepository extends ReactiveCrudRepository<PostItem, String> {
    Flux<PostItem> findByPostIdOrderByOrderNumberAsc(String postId);

    Mono<Void> deleteByPostId(String postId);
}