package com.dauducbach.clone.modules.post.stories.query;

import com.dauducbach.clone.modules.media.publicapi.MediaUrlDelivery;
import com.dauducbach.clone.modules.post.publicapi.StoryQuery;
import com.dauducbach.clone.modules.post.stories.policy.StoryExpiryPolicy;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class StoryQueryService implements StoryQuery {
    private final UserStoriesRepository storiesRepository;
    private final MediaUrlDelivery mediaAssets;

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
        if (!StoryExpiryPolicy.isApprovedAndActive(story, now)) {
            return new StoryAvailability(
                    reference.storyId(), story == null ? null : story.getUserId(), false,
                    story == null ? null : story.getMediaType(),
                    reference.previewAtMs(), StoryExpiryPolicy.effectiveExpiry(story), null);
        }
        String previewUrl = "VIDEO".equalsIgnoreCase(story.getMediaType())
                ? mediaAssets.storyVideoStill(story.getMediaUrl(), reference.previewAtMs())
                : story.getMediaUrl();
        return new StoryAvailability(
                story.getId(), story.getUserId(), true, story.getMediaType(), reference.previewAtMs(),
                StoryExpiryPolicy.effectiveExpiry(story), previewUrl);
    }
}
