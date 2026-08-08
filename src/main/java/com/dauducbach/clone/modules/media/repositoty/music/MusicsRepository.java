package com.dauducbach.clone.modules.media.repositoty.music;

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
            ORDER BY display_name ASC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<Musics> findPage(Pageable pageable);

    @Query("""
            SELECT * FROM musics
            WHERE LOWER(display_name) LIKE CONCAT('%', LOWER(:keyword), '%')
               OR LOWER(single_name) LIKE CONCAT('%', LOWER(:keyword), '%')
               OR LOWER(slug_name) LIKE CONCAT('%', LOWER(:keyword), '%')
            ORDER BY display_name ASC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<Musics> search(String keyword, Pageable pageable);

    @Query("""
            SELECT COUNT(*) FROM musics
            WHERE LOWER(display_name) LIKE CONCAT('%', LOWER(:keyword), '%')
               OR LOWER(single_name) LIKE CONCAT('%', LOWER(:keyword), '%')
               OR LOWER(slug_name) LIKE CONCAT('%', LOWER(:keyword), '%')
            """)
    Mono<Long> countSearch(String keyword);

    @Query("""
            SELECT * FROM musics
            WHERE LOWER(category) = LOWER(:category)
            ORDER BY display_name ASC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<Musics> findByCategory(String category, Pageable pageable);

    Mono<Long> countByCategory(String category);

    @Query("""
            SELECT * FROM musics
            WHERE LOWER(category) = LOWER(:category)
              AND (
                LOWER(display_name) LIKE CONCAT('%', LOWER(:keyword), '%')
                OR LOWER(single_name) LIKE CONCAT('%', LOWER(:keyword), '%')
                OR LOWER(slug_name) LIKE CONCAT('%', LOWER(:keyword), '%')
              )
            ORDER BY display_name ASC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<Musics> searchByCategory(String keyword, String category, Pageable pageable);

    @Query("""
            SELECT COUNT(*) FROM musics
            WHERE LOWER(category) = LOWER(:category)
              AND (
                LOWER(display_name) LIKE CONCAT('%', LOWER(:keyword), '%')
                OR LOWER(single_name) LIKE CONCAT('%', LOWER(:keyword), '%')
                OR LOWER(slug_name) LIKE CONCAT('%', LOWER(:keyword), '%')
              )
            """)
    Mono<Long> countSearchByCategory(String keyword, String category);
}
