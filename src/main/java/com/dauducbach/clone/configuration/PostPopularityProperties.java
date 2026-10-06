package com.dauducbach.clone.configuration;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Data
@Component
@ConfigurationProperties("post.popularity")
public class PostPopularityProperties {
    private boolean ingestionEnabled;
    private boolean streamsEnabled;
    private boolean projectionEnabled;
    private boolean mixedFeedEnabled;
    private int threshold = 20;
    private Duration windowSize = Duration.ofHours(5);
    private Duration hop = Duration.ofMinutes(5);
    private Duration grace = Duration.ofMinutes(10);
    private Duration windowRetention = Duration.ofHours(6);
    private Duration identityRetention = Duration.ofDays(8);
    private Duration impressionSpan = Duration.ofHours(2);
    private Duration popularLifetime = Duration.ofHours(48);
    private Duration cursorLifetime = Duration.ofMinutes(20);
    private int scanBatchSize = 40;
    private int maxScanPerSource = 400;
    private int partitions = 1;
    private String applicationId = "local-post-popularity-v1";
    private String likeTopic = "like_event";
    private String commentTopic = "comment_success_event";
    private String interactionTopic = "post_interaction";
    private String outputTopic = "post_popularity_updates";
    private String invalidTopic = "post_popularity_invalid";
    private String redisKey = "post_popular";
    private String cursorSecret = "";

    @PostConstruct
    public void validate() {
        if (threshold < 0 || partitions < 1 || scanBatchSize < 1 || maxScanPerSource < scanBatchSize
                || !positive(windowSize) || !positive(hop) || hop.compareTo(windowSize) > 0
                || grace == null || grace.isNegative() || windowRetention == null
                || windowRetention.compareTo(windowSize.plus(grace)) < 0
                || identityRetention == null || identityRetention.compareTo(Duration.ofDays(7)) <= 0
                || !positive(impressionSpan) || !positive(popularLifetime) || !positive(cursorLifetime)
                || applicationId == null || applicationId.isBlank() || redisKey == null || redisKey.isBlank()) {
            throw new IllegalArgumentException("Invalid post.popularity window, retention, quota or identity configuration");
        }
        if (mixedFeedEnabled && (cursorSecret == null || cursorSecret.length() < 32)) {
            throw new IllegalArgumentException("Mixed feed requires post.popularity.cursor-secret of at least 32 characters");
        }
    }

    private boolean positive(Duration duration) {
        return duration != null && !duration.isNegative() && !duration.isZero();
    }
}
