package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.chat.service.StoryAvailabilityPort;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repositoty.story.UserStoriesRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class ChatStoryAvailabilityAdapter implements StoryAvailabilityPort {
    private final UserStoriesRepository storiesRepository;
    private final MediaCompatibilityFacade mediaFacade;

    @Override
    public Mono<Map<StoryReference, StoryAvailability>> resolve(
            Collection<StoryReference> references,
            Instant now
    ) {
        Map<StoryReference, StoryAvailability> result = new LinkedHashMap<>();
        if (references == null || references.isEmpty()) {
            return Mono.just(result);
        }
        Map<String, UserStories> stories = new LinkedHashMap<>();
        return storiesRepository.findAllById(references.stream().map(StoryReference::storyId).distinct().toList())
                .doOnNext(story -> stories.put(story.getId(), story))
                .then(Mono.fromSupplier(() -> {
                    references.forEach(reference -> result.put(
                            reference,
                            availability(reference, stories.get(reference.storyId()), now)));
                    return Map.copyOf(result);
                }));
    }

    private StoryAvailability availability(StoryReference reference, UserStories story, Instant now) {
        if (story == null || !isAvailable(story, now)) {
            return new StoryAvailability(
                    reference.storyId(), false, story == null ? null : story.getMediaType(),
                    reference.previewAtMs(), effectiveExpiry(story), null);
        }
        String previewUrl = "VIDEO".equalsIgnoreCase(story.getMediaType())
                ? mediaFacade.storyVideoStill(story.getMediaUrl(), reference.previewAtMs())
                : story.getMediaUrl();
        return new StoryAvailability(
                story.getId(), true, story.getMediaType(), reference.previewAtMs(),
                effectiveExpiry(story), previewUrl);
    }

    private boolean isAvailable(UserStories story, Instant now) {
        Instant expiry = effectiveExpiry(story);
        return "APPROVED".equalsIgnoreCase(story.getStatus())
                && expiry != null
                && expiry.isAfter(now);
    }

    private Instant effectiveExpiry(UserStories story) {
        if (story == null) {
            return null;
        }
        return story.getExpiredAt() != null
                ? story.getExpiredAt()
                : story.getCreatedAt() == null ? null : story.getCreatedAt().plusSeconds(24 * 60 * 60);
    }
}
