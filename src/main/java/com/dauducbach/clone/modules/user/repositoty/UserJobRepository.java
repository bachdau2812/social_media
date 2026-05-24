package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserJob;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface UserJobRepository extends ReactiveCrudRepository<UserJob, String> {
    Flux<UserJob> findByUserId(String userId);
}