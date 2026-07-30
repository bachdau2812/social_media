package com.dauducbach.clone.modules.frontend.service;

import com.dauducbach.clone.modules.frontend.dto.ConnectionUserResponse;
import com.dauducbach.clone.modules.frontend.dto.ConnectionsResponse;
import com.dauducbach.clone.modules.frontend.dto.ProfilePostResponse;
import com.dauducbach.clone.modules.frontend.dto.ProfileSummaryResponse;
import com.dauducbach.clone.modules.post.service.PostProfileQueryService;
import com.dauducbach.clone.modules.user.service.UserIdentityQueryService;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.Locale;

@Service
@RequiredArgsConstructor
public class ProfileScreenService {
    private static final int CONNECTION_FETCH_LIMIT = 500;

    private final UserProfileCompositionQueryService profileQueryService;
    private final UserIdentityQueryService userIdentityQueryService;
    private final PostProfileQueryService postProfileQueryService;

    public Mono<ProfileSummaryResponse> getProfile(String viewerId, String userId, int postLimit) {
        int safePostLimit = postLimit <= 0 ? 12 : Math.min(postLimit, 50);
        String safeViewer = viewerId == null || viewerId.isBlank() ? userId : viewerId;
        boolean isOwner = safeViewer.equals(userId);

        Mono<List<ProfilePostResponse>> recentPosts =
                postProfileQueryService.getRecentPosts(safeViewer, userId, safePostLimit)
                        .map(this::toProfilePost)
                        .collectList()
                        .onErrorReturn(List.of());
        Mono<List<ProfilePostResponse>> repostedPosts =
                postProfileQueryService.getRepostedPosts(safeViewer, userId, safePostLimit)
                        .map(this::toProfilePost)
                        .collectList()
                        .onErrorReturn(List.of());

        return Mono.zip(
                profileQueryService.getProfileBundle(safeViewer, userId, isOwner),
                recentPosts,
                repostedPosts
        ).map(tuple -> {
            UserProfileCompositionQueryService.ProfileBundleSnapshot profile = tuple.getT1();
            UserProfileCompositionQueryService.ProfileRelationshipSnapshot relationship =
                    profile.relationship();
            return new ProfileSummaryResponse(
                    profile.user(),
                    profile.currentAvatar(),
                    relationship.followerCount(),
                    relationship.followingCount(),
                    relationship.friendCount(),
                    relationship.viewerFollowsUser(),
                    relationship.userFollowsViewer(),
                    relationship.viewerFollowsUser() && relationship.userFollowsViewer(),
                    relationship.socialMedia(),
                    profile.jobs(),
                    profile.universities(),
                    profile.highSchools(),
                    tuple.getT2(),
                    tuple.getT3()
            );
        });
    }

    public Mono<ConnectionsResponse> getConnections(
            String viewerId,
            String userId,
            String tab,
            String query,
            String sort,
            int page,
            int size
    ) {
        String safeViewer = viewerId == null || viewerId.isBlank() ? userId : viewerId;
        String safeTab = normalizeTab(tab);
        String safeQuery = query == null ? "" : query.trim().toLowerCase(Locale.ROOT);
        String safeSort = sort == null ? "RECENT" : sort.trim().toUpperCase(Locale.ROOT);
        int safePage = Math.max(page, 0);
        int safeSize = size <= 0 ? 20 : Math.min(size, 60);

        return profileQueryService.getConnectionRows(userId, safeTab, CONNECTION_FETCH_LIMIT)
                .onErrorResume(error -> Flux.empty())
                .flatMap(follow -> hydrateConnection(
                        safeViewer,
                        targetUserId(follow, safeTab),
                        follow.id(),
                        follow.createdAt() == null ? null : follow.createdAt().toString()))
                .filter(row -> matches(row, safeQuery))
                .collectList()
                .map(rows -> buildConnectionsResponse(
                        userId, safeTab, safeSort, safePage, safeSize, rows))
                .onErrorReturn(new ConnectionsResponse(
                        userId, safeTab, List.of(), 0, safePage, safeSize, false, safePage > 0));
    }

    private ProfilePostResponse toProfilePost(PostProfileQueryService.ProfilePostSnapshot post) {
        return new ProfilePostResponse(
                post.postId(),
                post.userId(),
                post.authorUsername(),
                post.authorFullName(),
                post.authorAvatarUrl(),
                post.content(),
                post.hashtags(),
                post.mediaRatio(),
                post.firstItem(),
                post.music(),
                post.likeCount(),
                post.commentCount(),
                post.repostCount(),
                post.likedByCurrentUser(),
                post.repostedByCurrentUser(),
                post.createdAt(),
                post.updatedAt()
        );
    }

    private Mono<ConnectionUserResponse> hydrateConnection(
            String viewerId,
            String targetUserId,
            String followId,
            String followedAt
    ) {
        Mono<Boolean> viewerFollows = profileQueryService.isFollowing(viewerId, targetUserId)
                .onErrorReturn(false);
        Mono<Boolean> followsViewer = profileQueryService.isFollowing(targetUserId, viewerId)
                .onErrorReturn(false);

        return Mono.zip(
                        userIdentityQueryService.findIdentity(targetUserId),
                        viewerFollows,
                        followsViewer
                )
                .map(tuple -> {
                    UserIdentityQueryService.IdentitySnapshot user = tuple.getT1();
                    boolean isFriend = Boolean.TRUE.equals(tuple.getT2())
                            && Boolean.TRUE.equals(tuple.getT3());
                    return new ConnectionUserResponse(
                            followId,
                            user.userId(),
                            user.username(),
                            user.fullName(),
                            user.avatarUrl(),
                            mutualContext(
                                    viewerId,
                                    targetUserId,
                                    Boolean.TRUE.equals(tuple.getT2()),
                                    Boolean.TRUE.equals(tuple.getT3()),
                                    isFriend),
                            relationshipAction(
                                    viewerId,
                                    targetUserId,
                                    Boolean.TRUE.equals(tuple.getT2()),
                                    Boolean.TRUE.equals(tuple.getT3()),
                                    isFriend),
                            Boolean.TRUE.equals(tuple.getT2()),
                            Boolean.TRUE.equals(tuple.getT3()),
                            isFriend,
                            followedAt
                    );
                })
                .onErrorResume(error -> Mono.empty());
    }

    private ConnectionsResponse buildConnectionsResponse(
            String userId,
            String tab,
            String sort,
            int page,
            int size,
            List<ConnectionUserResponse> rows
    ) {
        if ("NAME".equals(sort)) {
            rows.sort(Comparator.comparing(row -> row.username().toLowerCase(Locale.ROOT)));
        } else {
            rows.sort(Comparator.comparing(
                    (ConnectionUserResponse row) -> row.followedAt() == null ? "" : row.followedAt()
            ).reversed());
        }
        int total = rows.size();
        int from = Math.min(page * size, total);
        int to = Math.min(from + size, total);
        return new ConnectionsResponse(
                userId, tab, rows.subList(from, to), total, page, size, to < total, page > 0);
    }

    private boolean matches(ConnectionUserResponse row, String query) {
        if (query.isBlank()) {
            return true;
        }
        return row.username().toLowerCase(Locale.ROOT).contains(query)
                || row.displayName().toLowerCase(Locale.ROOT).contains(query);
    }

    private String normalizeTab(String tab) {
        if (tab == null) {
            return "FOLLOWERS";
        }
        return switch (tab.trim().toUpperCase(Locale.ROOT)) {
            case "FOLLOWING" -> "FOLLOWING";
            case "FRIENDS" -> "FRIENDS";
            default -> "FOLLOWERS";
        };
    }

    private String targetUserId(
            UserProfileCompositionQueryService.ConnectionSnapshot follow,
            String tab
    ) {
        return "FOLLOWERS".equals(tab) ? follow.followerId() : follow.followingId();
    }

    private String relationshipAction(
            String viewerId,
            String targetUserId,
            boolean viewerFollows,
            boolean followsViewer,
            boolean friend
    ) {
        if (targetUserId.equals(viewerId)) {
            return "You";
        }
        if (friend) {
            return "Friend";
        }
        if (viewerFollows) {
            return "Following";
        }
        if (followsViewer) {
            return "Follow back";
        }
        return "Follow";
    }

    private String mutualContext(
            String viewerId,
            String targetUserId,
            boolean viewerFollows,
            boolean followsViewer,
            boolean friend
    ) {
        if (targetUserId.equals(viewerId)) {
            return "Current account";
        }
        if (friend) {
            return "Mutual follow";
        }
        if (followsViewer) {
            return "Follows you";
        }
        if (viewerFollows) {
            return "You follow this account";
        }
        return "Suggested connection";
    }
}
