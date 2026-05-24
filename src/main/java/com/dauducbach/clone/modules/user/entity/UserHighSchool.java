package com.dauducbach.clone.modules.user.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Table("user_high_school")
public class UserHighSchool {
    @Id
    String id;
    String userId;
    String schoolName;
    LocalDate fromDate;
    LocalDate toDate;
    boolean isGraduate;
    boolean isPublic;
}
