package com.dauducbach.clone.modules.frontend.service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class StorySeenHydrator {
    private StorySeenHydrator() {
    }

    record StoryOwner(String storyId, String ownerId) {
    }

    static Set<String> seenIds(String viewerId, List<StoryOwner> stories, Set<String> persistedSeenIds) {
        Set<String> seenIds = new HashSet<>(persistedSeenIds);
        stories.stream()
                .filter(story -> viewerId.equals(story.ownerId()))
                .map(StoryOwner::storyId)
                .forEach(seenIds::add);
        return seenIds;
    }
}
