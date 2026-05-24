package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface UserHighSchoolRepository extends ReactiveCrudRepository<UserHighSchool, String> {
    Flux<UserHighSchool> findByUserId(String userId);
}