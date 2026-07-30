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
            WHERE LOWER(COALESCE(username, '')) LIKE CONCAT('%', LOWER(:query), '%')
               OR (:includeHobby = 1 AND LOWER(COALESCE(hobby_list, '')) LIKE CONCAT('%', LOWER(:query), '%'))
               OR (:includeLivingIn = 1 AND LOWER(COALESCE(living_in, '')) LIKE CONCAT('%', LOWER(:query), '%'))
               OR (:includeHometown = 1 AND LOWER(COALESCE(hometown, '')) LIKE CONCAT('%', LOWER(:query), '%'))
               OR (:includeSex = 1 AND LOWER(COALESCE(sex, '')) LIKE CONCAT('%', LOWER(:query), '%'))
            ORDER BY username ASC, user_id ASC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<String> searchUserIds(String query,
                               int includeHobby,
                               int includeLivingIn,
                               int includeHometown,
                               int includeSex,
                               Pageable pageable);

    @Query("""
            SELECT COUNT(*) FROM user_details
            WHERE LOWER(COALESCE(username, '')) LIKE CONCAT('%', LOWER(:query), '%')
               OR (:includeHobby = 1 AND LOWER(COALESCE(hobby_list, '')) LIKE CONCAT('%', LOWER(:query), '%'))
               OR (:includeLivingIn = 1 AND LOWER(COALESCE(living_in, '')) LIKE CONCAT('%', LOWER(:query), '%'))
               OR (:includeHometown = 1 AND LOWER(COALESCE(hometown, '')) LIKE CONCAT('%', LOWER(:query), '%'))
               OR (:includeSex = 1 AND LOWER(COALESCE(sex, '')) LIKE CONCAT('%', LOWER(:query), '%'))
            """)
    Mono<Long> countSearchUserIds(String query,
                                  int includeHobby,
                                  int includeLivingIn,
                                  int includeHometown,
                                  int includeSex);
}
