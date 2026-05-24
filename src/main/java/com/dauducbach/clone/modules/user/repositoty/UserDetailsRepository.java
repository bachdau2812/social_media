package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserDetails;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserDetailsRepository extends ReactiveCrudRepository<UserDetails, String> {
    // ReactiveCrudRepository already provides existsById(String id) method
}
