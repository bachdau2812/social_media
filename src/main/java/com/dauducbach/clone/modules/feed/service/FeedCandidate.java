package com.dauducbach.clone.modules.feed.service;

public record FeedCandidate(
        String postId,
        String sourceType,
        String recommendationReason,
        double deliveryScore,
        Long sourceOrderTimeMillis,
        java.time.Instant sourceOrderTime
 ) {
    public FeedCandidate(String postId, String sourceType, String recommendationReason, double deliveryScore) {
        this(postId, sourceType, recommendationReason, deliveryScore, null, null);
    }
    public FeedCandidate(String postId, String sourceType, String recommendationReason, double deliveryScore, Long sourceOrderTimeMillis) {
        this(postId, sourceType, recommendationReason, deliveryScore, sourceOrderTimeMillis, null);
    }

}