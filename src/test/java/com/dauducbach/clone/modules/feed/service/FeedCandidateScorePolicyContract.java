package com.dauducbach.clone.modules.feed.service;

public final class FeedCandidateScorePolicyContract {
    private FeedCandidateScorePolicyContract() {
    }

    public static void main(String[] args) {
        int candidateCount = 3;

        double first = FeedCandidateScorePolicy.score(candidateCount, 0);
        double second = FeedCandidateScorePolicy.score(candidateCount, 1);
        double third = FeedCandidateScorePolicy.score(candidateCount, 2);

        if (!(first > second && second > third)) {
            throw new AssertionError("Candidate scores must preserve source order for Redis reverseRange");
        }
        if (FeedCandidateScorePolicy.score(candidateCount, 0) != first) {
            throw new AssertionError("Candidate score must be deterministic for the same ordered batch");
        }
    }
}
