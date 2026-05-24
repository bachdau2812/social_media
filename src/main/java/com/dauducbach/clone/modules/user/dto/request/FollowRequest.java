package com.dauducbach.clone.modules.user.dto.request;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

public class FollowRequest {
    @NotBlank(message = "Follower ID must not be blank")
    String followerId;

    @NotBlank(message = "Following ID must not be blank")
    String followingId;
}