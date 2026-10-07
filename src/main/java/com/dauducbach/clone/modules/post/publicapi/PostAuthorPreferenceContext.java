package com.dauducbach.clone.modules.post.publicapi;

import java.util.List;

/** Minimal user preference snapshot needed to build a post recommendation vector. */
public record PostAuthorPreferenceContext(long version, List<Double> profile, List<Double> longTerm, String model) {
    public PostAuthorPreferenceContext {
        profile = List.copyOf(profile);
        longTerm = List.copyOf(longTerm);
    }
}
