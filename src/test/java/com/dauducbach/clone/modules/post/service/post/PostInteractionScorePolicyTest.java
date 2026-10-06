package com.dauducbach.clone.modules.post.service.post;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PostInteractionScorePolicyTest {
    private final PostInteractionScorePolicy policy = new PostInteractionScorePolicy();

    @Test void strictDwellBoundariesAndClick() {
        assertEquals(0, policy.score(false, 0));
        assertEquals(0, policy.score(false, 30));
        assertEquals(1, policy.score(false, 31));
        assertEquals(1, policy.score(false, 60));
        assertEquals(2, policy.score(false, 61));
        assertEquals(3, policy.score(true, 3600));
    }

    @Test void rejectsSecondsOutsideContract() {
        assertThrows(IllegalArgumentException.class, () -> policy.score(false, -1));
        assertThrows(IllegalArgumentException.class, () -> policy.score(true, 3601));
    }
}
