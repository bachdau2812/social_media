package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.Media;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
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
    Flux<Media> findByOwnerIdAndOwnerType(String ownerId, com.dauducbach.clone.modules.post.constant.OwnerType ownerType);

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
    Mono<Void> deleteByOwnerIdAndOwnerType(String ownerId, com.dauducbach.clone.modules.post.constant.OwnerType ownerType);
}