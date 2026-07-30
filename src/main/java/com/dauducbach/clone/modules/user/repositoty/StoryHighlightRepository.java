package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.StoryHighlight;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface StoryHighlightRepository extends ReactiveCrudRepository<StoryHighlight, String> {
    Flux<StoryHighlight> findByOwnerIdOrderByUpdatedAtDesc(String ownerId);
}
