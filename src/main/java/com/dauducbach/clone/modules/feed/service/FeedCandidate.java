package com.dauducbach.clone.modules.feed.service;

public record FeedCandidate(
        String postId,
        String sourceType,
        String recommendationReason,
        double deliveryScore
) {
}