package com.dauducbach.clone.modules.user.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.time.LocalDate;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

public class UserDetailsUpdateRequest {
    String userId;
    String username;
    LocalDate dob;
    String homeTown;
    String livingIn;
    String sex;
    List<String> hobbieList;
}