package com.dauducbach.clone.modules.media.repository;

import com.dauducbach.clone.modules.media.entity.Media;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface MediaRepository extends R2dbcRepository<Media, String> {

    /**
     * Find all media by owner ID and owner type
     * @param ownerId the owner ID (post ID or comment ID)
     * @param ownerType the owner type (POST or COMMENT)
     * @return Flux of Media
     */
    Flux<Media> findByOwnerIdAndOwnerType(String ownerId, com.dauducbach.clone.modules.media.constant.OwnerType ownerType);

    Flux<Media> findByOwnerIdAndOwnerTypeOrderByCreatedAtDesc(String ownerId, com.dauducbach.clone.modules.media.constant.OwnerType ownerType, Pageable pageable);

    Mono<Long> countByOwnerIdAndOwnerType(String ownerId, com.dauducbach.clone.modules.media.constant.OwnerType ownerType);

    Mono<Media> findFirstByOwnerIdAndOwnerTypeOrderByCreatedAtDesc(String ownerId, com.dauducbach.clone.modules.media.constant.OwnerType ownerType);

    Mono<Media> findFirstByOwnerIdAndOwnerTypeOrderByCreatedAtAsc(String ownerId, com.dauducbach.clone.modules.media.constant.OwnerType ownerType);

    @Query("""
            SELECT m.*
            FROM media m
            JOIN post_details p ON p.post_id = m.owner_id
            WHERE p.user_id = :userId AND m.owner_type = 'POST'
            ORDER BY m.created_at DESC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<Media> findPostMediaByUserId(String userId, Pageable pageable);

    @Query("""
            SELECT COUNT(*)
            FROM media m
            JOIN post_details p ON p.post_id = m.owner_id
            WHERE p.user_id = :userId AND m.owner_type = 'POST'
            """)
    Mono<Long> countPostMediaByUserId(String userId);

    /**
     * Find all media by owner ID
     * @param ownerId the owner ID (post ID or comment ID)
     * @return Flux of Media
     */
    Flux<Media> findByOwnerId(String ownerId);

    /**
     * Find all media by public ID
     * @param publicId the public ID from Cloudinary
     * @return Mono of Media
     */
    Mono<Media> findByPublicId(String publicId);

    /**
     * Delete all media by owner ID and owner type
     * @param ownerId the owner ID (post ID or comment ID)
     * @param ownerType the owner type (POST or COMMENT)
     * @return Mono indicating completion
     */
    Mono<Void> deleteByOwnerIdAndOwnerType(String ownerId, com.dauducbach.clone.modules.media.constant.OwnerType ownerType);
}
