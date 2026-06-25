package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserUniversity;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface UserUniversityRepository extends ReactiveCrudRepository<UserUniversity, String> {
    @Query("""
            SELECT id, user_id, school_name, major, `from`, `to`, is_graduate, is_public
            FROM user_university
            WHERE user_id = :userId
            """)
    Flux<UserUniversity> findByUserId(String userId);
}
