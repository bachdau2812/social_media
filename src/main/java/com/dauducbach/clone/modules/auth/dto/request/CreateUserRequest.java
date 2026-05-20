package com.dauducbach.clone.modules.auth.dto.request;

import jakarta.validation.constraints.NotBlank;
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
    @NotBlank(message = "Username must not be blank")
    String username;

    @NotBlank(message = "Password must not be blank")
    String password;

    @NotBlank(message = "Email must not be blank")
    String email;

    String phoneNumber;
    Date birthday;
    String sex;
    String currentAddress;
    String homeTown;

    @Builder.Default
    String role = "USER";
}
