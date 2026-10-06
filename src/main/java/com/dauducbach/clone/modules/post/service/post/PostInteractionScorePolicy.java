package com.dauducbach.clone.modules.post.service.post;

import org.springframework.stereotype.Component;

@Component
public class PostInteractionScorePolicy {
    public int score(boolean click, int seconds) {
        if (seconds < 0 || seconds > 3600) {
            throw new IllegalArgumentException("viewTime must be from 0 to 3600 seconds");
        }
        int dwellScore = seconds > 60 ? 2 : seconds > 30 ? 1 : 0;
        return (click ? 1 : 0) + dwellScore;
    }
}
