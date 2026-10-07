package com.dauducbach.clone.modules.post.stories.publishing;

import com.dauducbach.clone.modules.post.entity.story.UserStories;
import reactor.core.publisher.Mono;

public interface StoryPublishingStore {
    Mono<Submission> createOrReuse(UserStories story, boolean identifyPublication);

    Mono<UserStories> save(UserStories story);

    record Submission(UserStories story, boolean shouldScan) { }
}
