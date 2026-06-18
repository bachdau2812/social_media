package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserStories;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface UserStoriesRepository extends ReactiveCrudRepository<UserStories, String> {
    Flux<UserStories> findByUserIdOrderByCreatedAtDesc(String userId, Pageable pageable);

    Mono<Long> countByUserId(String userId);
}
