package com.dauducbach.clone.modules.frontend.service;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class StorySeenHydratorTest {
    @Test
    void treatsOwnedAndPersistedStoryIdsAsSeen() {
        Set<String> seen = StorySeenHydrator.seenIds(
                "viewer-1",
                List.of(
                        new StorySeenHydrator.StoryOwner("own", "viewer-1"),
                        new StorySeenHydrator.StoryOwner("viewed", "owner-1"),
                        new StorySeenHydrator.StoryOwner("unseen", "owner-1")
                ),
                Set.of("viewed")
        );

        assertThat(seen).containsExactlyInAnyOrder("own", "viewed");
    }
}
