package com.dauducbach.clone.modules.feed.service;

public final class FeedCandidateScorePolicy {
    private FeedCandidateScorePolicy() {
    }

    public static double score(int candidateCount, int index) {
        if (candidateCount < 0) {
            throw new IllegalArgumentException("candidateCount must not be negative");
        }
        if (index < 0 || index >= candidateCount) {
            throw new IllegalArgumentException("index must reference a candidate in the batch");
        }
        return candidateCount - index;
    }
}