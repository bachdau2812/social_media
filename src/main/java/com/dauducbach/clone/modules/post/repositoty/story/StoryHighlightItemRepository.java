package com.dauducbach.clone.modules.post.repositoty.story;

import com.dauducbach.clone.modules.post.entity.story.StoryHighlightItem;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface StoryHighlightItemRepository extends ReactiveCrudRepository<StoryHighlightItem, String> {
    Flux<StoryHighlightItem> findByHighlightIdOrderByOrderNumberAsc(String highlightId);
    Mono<Void> deleteByHighlightId(String highlightId);
}
