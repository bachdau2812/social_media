package com.dauducbach.clone.modules.post.repositoty.story;

import com.dauducbach.clone.modules.post.entity.story.StoryHighlight;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface StoryHighlightRepository extends ReactiveCrudRepository<StoryHighlight, String> {
    Flux<StoryHighlight> findByOwnerIdOrderByUpdatedAtDesc(String ownerId);
}
