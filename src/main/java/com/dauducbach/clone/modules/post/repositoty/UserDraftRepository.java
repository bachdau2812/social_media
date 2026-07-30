package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.UserDraft;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface UserDraftRepository extends ReactiveCrudRepository<UserDraft, String> {
    @Query("SELECT * FROM user_drafts WHERE user_id = :userId ORDER BY updated_at DESC, id DESC")
    Flux<UserDraft> findByUserId(String userId);
}