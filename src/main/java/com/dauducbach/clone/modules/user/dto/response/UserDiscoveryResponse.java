package com.dauducbach.clone.modules.user.dto.response;

public record UserDiscoveryResponse(
        String userId,
        String username,
        String fullName,
        String avatarUrl,
        boolean viewerFollowsUser,
        boolean userFollowsViewer,
        boolean friend,
        String relationship
) {
}
