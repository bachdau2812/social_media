package com.dauducbach.clone.modules.frontend.dto;

public record ConnectionUserResponse(
        String id,
        String userId,
        String username,
        String displayName,
        String avatarUrl,
        String mutualContext,
        String relationshipAction,
        boolean viewerFollowsUser,
        boolean userFollowsViewer,
        boolean friend,
        String followedAt
) {
}
