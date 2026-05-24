package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserUniversity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface UserUniversityRepository extends ReactiveCrudRepository<UserUniversity, String> {
    Flux<UserUniversity> findByUserId(String userId);
}