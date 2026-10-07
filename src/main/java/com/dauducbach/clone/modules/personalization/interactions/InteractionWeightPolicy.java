package com.dauducbach.clone.modules.personalization.interactions;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.Locale;

@Component
public class InteractionWeightPolicy {
    private final double repostShort;
    private final double repostLong;

    public InteractionWeightPolicy(@Value("${vector.interaction.repost-short-weight:1.0}") double repostShort,
                                      @Value("${vector.interaction.repost-long-weight:0.4}") double repostLong) {
        if (!Double.isFinite(repostShort) || !Double.isFinite(repostLong) || repostShort < 0 || repostLong < 0) {
            throw new IllegalArgumentException("Invalid repost weights");
        }
        this.repostShort = repostShort;
        this.repostLong = repostLong;
    }

    public double shortWeight(String action) {
        return switch (normalize(action)) {
            case "LIKE", "LIKE_POST" -> 0.3;
            case "COMMENT", "COMMENT_POST" -> 0.7;
            case "REPOST", "REPOST_POST" -> repostShort;
            default -> throw new IllegalArgumentException("Unsupported interaction action: " + action);
        };
    }

    public double longWeight(String action) {
        return switch (normalize(action)) {
            case "LIKE", "LIKE_POST" -> 0.2;
            case "COMMENT", "COMMENT_POST" -> 0.3;
            case "REPOST", "REPOST_POST" -> repostLong;
            default -> throw new IllegalArgumentException("Unsupported interaction action: " + action);
        };
    }

    private String normalize(String action) {
        return action == null ? "" : action.trim().toUpperCase(Locale.ROOT);
    }
}
