package com.dauducbach.clone.modules.personalization.publicapi;

import java.util.List;

/** Immutable preference values captured at one committed version. */
public record PreferenceSnapshot(long version, List<Double> profile, List<Double> longTerm,
                                 List<Double> shortTerm, boolean hasLearnedHistory, String model) {
    public PreferenceSnapshot {
        profile = List.copyOf(profile);
        longTerm = List.copyOf(longTerm);
        shortTerm = List.copyOf(shortTerm);
    }
}
