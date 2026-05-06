package com.dauducbach.clone.modules.auth.dto.request;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

public class CreateUserRequest {
    String username;
    String password;
    String email;
    String phoneNumber;
    Date birthday;
    String sex;
    String currentAddress;
    String homeTown;

    @Builder.Default
    String role = "USER";
}
