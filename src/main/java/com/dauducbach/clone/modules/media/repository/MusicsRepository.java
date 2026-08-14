package com.dauducbach.clone.modules.media.repository;

import com.dauducbach.clone.modules.media.entity.music.Musics;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface MusicsRepository extends ReactiveCrudRepository<Musics, String> {
    Mono<Musics> findBySlugName(String slugName);

    Mono<Musics> findBySlugNameAndDisplayName(String slugName, String displayName);

    @Query("""
            SELECT * FROM musics
            ORDER BY popularity DESC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<Musics> findPage(Pageable pageable);

    @Query("""
            SELECT * FROM musics
            WHERE display_name LIKE :keywordPattern
               OR single_name LIKE :keywordPattern
               OR slug_name LIKE :keywordPattern
            ORDER BY popularity DESC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<Musics> search(String keywordPattern, Pageable pageable);

    @Query("""
            SELECT COUNT(*) FROM musics
            WHERE display_name LIKE :keywordPattern
               OR single_name LIKE :keywordPattern
               OR slug_name LIKE :keywordPattern
            """)
    Mono<Long> countSearch(String keywordPattern);

    @Query("""
            SELECT * FROM musics
            WHERE category = :category
            ORDER BY popularity DESC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<Musics> findByCategory(String category, Pageable pageable);

    Mono<Long> countByCategory(String category);

    @Query("""
            SELECT * FROM musics
            WHERE category = :category
              AND (
                display_name LIKE :keywordPattern
                OR single_name LIKE :keywordPattern
                OR slug_name LIKE :keywordPattern
              )
            ORDER BY popularity DESC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<Musics> searchByCategory(String keywordPattern, String category, Pageable pageable);

    @Query("""
            SELECT COUNT(*) FROM musics
            WHERE category = :category
              AND (
                display_name LIKE :keywordPattern
                OR single_name LIKE :keywordPattern
                OR slug_name LIKE :keywordPattern
              )
            """)
    Mono<Long> countSearchByCategory(String keywordPattern, String category);
}
