package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserSearchHistory;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface UserSearchHistoryRepository extends ReactiveCrudRepository<UserSearchHistory, String> {
    @Query("""
            SELECT *
            FROM user_search_histories
            WHERE user_id = :userId
              AND normalized_keyword = :normalizedKeyword
            LIMIT 1
            """)
    Mono<UserSearchHistory> findByUserIdAndNormalizedKeyword(String userId, String normalizedKeyword);

    @Query("""
            SELECT * FROM user_search_histories
            WHERE user_id = :userId
            ORDER BY last_searched_at DESC
            LIMIT :limit
            """)
    Flux<UserSearchHistory> findRecentActiveByUserId(String userId, int limit);

    @Modifying
    @Query("""
            INSERT INTO user_search_histories (
                id,
                user_id,
                keyword,
                normalized_keyword,
                search_count,
                created_at,
                last_searched_at
            )
            VALUES (
                :id,
                :userId,
                :keyword,
                :normalizedKeyword,
                1,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            """)
    Mono<Integer> insertHistory(String id, String userId, String keyword, String normalizedKeyword);

    @Modifying
    @Query("""
            UPDATE user_search_histories
            SET keyword = :keyword,
                search_count = search_count + 1,
                last_searched_at = CURRENT_TIMESTAMP
            WHERE id = :id
            """)
    Mono<Integer> incrementHistoryById(String id, String keyword);

    @Modifying
    @Query("""
            DELETE FROM user_search_histories
            WHERE user_id = :userId
              AND normalized_keyword = :normalizedKeyword
            """)
    Mono<Integer> deleteKeyword(String userId, String normalizedKeyword);

    @Modifying
    @Query("""
            DELETE FROM user_search_histories
            WHERE user_id = :userId
            """)
    Mono<Integer> deleteAllByUserId(String userId);
}
