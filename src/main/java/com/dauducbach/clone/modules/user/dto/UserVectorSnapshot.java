package com.dauducbach.clone.modules.user.dto;

import java.util.List;

public record UserVectorSnapshot(long version, List<Double> profile, List<Double> longTerm,
                                 List<Double> shortTerm, boolean hasLearnedHistory, String model) {
    public UserVectorSnapshot {
        profile = List.copyOf(profile); longTerm = List.copyOf(longTerm); shortTerm = List.copyOf(shortTerm);
    }
}
