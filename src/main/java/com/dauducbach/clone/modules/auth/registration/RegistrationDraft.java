package com.dauducbach.clone.modules.auth.registration;

import java.util.Date;
import java.util.List;

public record RegistrationDraft(
        String fullName,
        String username,
        String password,
        String email,
        String phoneNumber,
        Date dob,
        String sex,
        String livingIn,
        String hometown,
        List<String> hobbyList,
        String role) {
    public RegistrationDraft {
        hobbyList = hobbyList == null ? List.of() : List.copyOf(hobbyList);
    }
}
