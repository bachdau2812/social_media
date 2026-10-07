package com.dauducbach.clone.modules.user.publicapi;

public record UserIdentity(
        String userId,
        String username,
        String fullName,
        String avatarUrl) {
}
