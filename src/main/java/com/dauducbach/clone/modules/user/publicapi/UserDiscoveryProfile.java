package com.dauducbach.clone.modules.user.publicapi;

import java.util.List;

/** Minimal profile inputs used by personalization ranking, without exposing the user entity. */
public record UserDiscoveryProfile(String userId, String livingIn, String hometown, List<String> hobbies) {
    public UserDiscoveryProfile {
        hobbies = hobbies == null ? List.of() : List.copyOf(hobbies);
    }
}
