package com.dauducbach.clone.modules.post.repository;

import com.dauducbach.clone.modules.post.entity.PostItem;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

@Repository
public interface PostItemRepository extends ReactiveCrudRepository<PostItem, String> {
    Flux<PostItem> findByPostIdOrderByOrderNumberAsc(String postId);

    Flux<PostItem> findByPostIdInOrderByPostIdAscOrderNumberAsc(Collection<String> postIds);

    Mono<Void> deleteByPostId(String postId);
}
