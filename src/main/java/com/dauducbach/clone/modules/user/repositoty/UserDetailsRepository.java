package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserDetails;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface UserDetailsRepository extends ReactiveCrudRepository<UserDetails, String> {
    @Query("SELECT user_id FROM user_details ORDER BY user_id")
    Flux<String> findAllUserIds();

    @Query("""
            SELECT user_id FROM user_details
            WHERE username LIKE :queryPattern
               OR (:includeHobby = 1 AND hobby_list LIKE :queryPattern)
               OR (:includeLivingIn = 1 AND living_in LIKE :queryPattern)
               OR (:includeHometown = 1 AND hometown LIKE :queryPattern)
               OR (:includeSex = 1 AND sex LIKE :queryPattern)
            ORDER BY username ASC, user_id ASC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<String> searchUserIds(String queryPattern,
                               int includeHobby,
                               int includeLivingIn,
                               int includeHometown,
                               int includeSex,
                               Pageable pageable);

    @Query("""
            SELECT COUNT(*) FROM user_details
            WHERE username LIKE :queryPattern
               OR (:includeHobby = 1 AND hobby_list LIKE :queryPattern)
               OR (:includeLivingIn = 1 AND living_in LIKE :queryPattern)
               OR (:includeHometown = 1 AND hometown LIKE :queryPattern)
               OR (:includeSex = 1 AND sex LIKE :queryPattern)
            """)
    Mono<Long> countSearchUserIds(String queryPattern,
                                  int includeHobby,
                                  int includeLivingIn,
                                  int includeHometown,
                                  int includeSex);
}
