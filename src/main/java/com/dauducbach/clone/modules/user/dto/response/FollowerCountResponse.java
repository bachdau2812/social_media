package com.dauducbach.clone.modules.user.dto.response;

public record FollowerCountResponse(String userId, int followersCount, int followingCount) {
}
