package com.dauducbach.clone.modules.post.stories.infrastructure.persistence;

import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import com.dauducbach.clone.modules.post.stories.publishing.StoryPublishingStore;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class R2dbcStoryPublishingStore implements StoryPublishingStore {
    private final UserStoriesRepository userStoriesRepository;
    private final R2dbcEntityTemplate entityTemplate;

    @Override
    public Mono<Submission> createOrReuse(UserStories story, boolean identifyPublication) {
        Mono<Submission> existing = identifyPublication
                ? findByPublication(story).map(found -> new Submission(found, false))
                : Mono.empty();
        Mono<Submission> insertOnce = Mono.defer(() -> entityTemplate.insert(UserStories.class).using(story)
                .map(saved -> new Submission(saved, true))
                .onErrorResume(DataIntegrityViolationException.class, error ->
                        findByPublication(story)
                                .map(found -> new Submission(found, false))
                                .switchIfEmpty(Mono.error(error))));
        return existing.switchIfEmpty(insertOnce);
    }

    @Override
    public Mono<UserStories> save(UserStories story) {
        return userStoriesRepository.save(story);
    }

    private Mono<UserStories> findByPublication(UserStories story) {
        return userStoriesRepository.findByUserIdAndPublicationIdAndPublicationOrder(
                story.getUserId(), story.getPublicationId(), story.getPublicationOrder());
    }
}
