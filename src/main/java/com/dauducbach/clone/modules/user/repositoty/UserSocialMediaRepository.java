package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserSocialMedia;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface UserSocialMediaRepository extends ReactiveCrudRepository<UserSocialMedia, String> {
    Flux<UserSocialMedia> findByUserId(String userId);
}