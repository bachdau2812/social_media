package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserPhone;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface UserPhoneRepository extends ReactiveCrudRepository<UserPhone, String> {
    Flux<UserPhone> findByUserId(String userId);
}