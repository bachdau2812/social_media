package com.dauducbach.clone.modules.user.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

public class UserUniversityRequest {
    String userId;
    String schoolName;
    String major;
    LocalDate from;
    LocalDate to;
    Boolean isGraduate;
    Boolean isPublic;
}