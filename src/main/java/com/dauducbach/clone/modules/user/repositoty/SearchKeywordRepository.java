package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.SearchKeyword;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface SearchKeywordRepository extends ReactiveCrudRepository<SearchKeyword, Long> {
    @Query("""
            SELECT * FROM search_keywords
            WHERE normalized_keyword LIKE :prefixPattern
              AND user_count >= :minUserCount
            ORDER BY search_count DESC, updated_at DESC
            LIMIT :limit
            """)
    Flux<SearchKeyword> findPublicByPrefix(String prefixPattern, long minUserCount, int limit);

    @Query("""
            SELECT * FROM search_keywords
            WHERE user_count >= :minUserCount
            ORDER BY search_count DESC, updated_at DESC
            LIMIT :limit
            """)
    Flux<SearchKeyword> findPopular(long minUserCount, int limit);

    @Modifying
    @Query("""
            INSERT INTO search_keywords (
                keyword,
                normalized_keyword,
                search_count,
                user_count,
                created_at,
                updated_at
            )
            VALUES (
                :keyword,
                :normalizedKeyword,
                1,
                :userCountDelta,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
            ON DUPLICATE KEY UPDATE
                keyword = VALUES(keyword),
                search_count = search_count + 1,
                user_count = user_count + :userCountDelta,
                updated_at = CURRENT_TIMESTAMP
            """)
    Mono<Integer> upsertKeyword(String keyword, String normalizedKeyword, int userCountDelta);
}
