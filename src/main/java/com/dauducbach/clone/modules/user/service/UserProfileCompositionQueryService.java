package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.entity.UserFollower;
import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import com.dauducbach.clone.modules.user.entity.UserJob;
import com.dauducbach.clone.modules.user.entity.UserSocialMedia;
import com.dauducbach.clone.modules.user.entity.UserUniversity;
import com.dauducbach.clone.modules.user.repositoty.UserFollowerRepository;
import com.dauducbach.clone.modules.user.repositoty.UserSocialMediaRepository;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class UserProfileCompositionQueryService {
    private final UserFollowerRepository followerRepository;
    private final UserSocialMediaRepository socialMediaRepository;
    private final UserDetailsService userDetailsService;
    private final UserJobService userJobService;
    private final UserUniversityService userUniversityService;
    private final UserHighSchoolService userHighSchoolService;
    private final MediaForProfile mediaForProfile;

    public Mono<ProfileBundleSnapshot> getProfileBundle(String viewerId, String userId, boolean isOwner) {
        Mono<Optional<MediaSnapshot>> avatar = mediaForProfile.getCurrentAvatar(userId)
                .map(this::toMediaSnapshot)
                .map(Optional::of)
                .onErrorResume(error -> Mono.empty())
                .defaultIfEmpty(Optional.empty());

        return Mono.zip(
                userDetailsService.getUserDetailsById(userId).map(this::toUserSnapshot),
                avatar,
                getRelationshipSnapshot(viewerId, userId),
                userJobService.getUserJobsByUserId(userId, isOwner)
                        .map(this::toJobSnapshot)
                        .collectList()
                        .onErrorReturn(List.of()),
                userUniversityService.getUserUniversitiesByUserId(userId, isOwner)
                        .map(this::toUniversitySnapshot)
                        .collectList()
                        .onErrorReturn(List.of()),
                userHighSchoolService.getUserHighSchoolsByUserId(userId, isOwner)
                        .map(this::toHighSchoolSnapshot)
                        .collectList()
                        .onErrorReturn(List.of())
        ).map(tuple -> new ProfileBundleSnapshot(
                tuple.getT1(),
                tuple.getT2().orElse(null),
                tuple.getT3(),
                tuple.getT4(),
                tuple.getT5(),
                tuple.getT6()
        ));
    }

    public Mono<ProfileRelationshipSnapshot> getRelationshipSnapshot(String viewerId, String userId) {
        return Mono.zip(
                followerRepository.countFollowers(userId).defaultIfEmpty(0L),
                followerRepository.countFollowing(userId).defaultIfEmpty(0L),
                followerRepository.countFriends(userId).defaultIfEmpty(0L),
                followerRepository.existsByFollowerIdAndFollowingId(viewerId, userId).defaultIfEmpty(false),
                followerRepository.existsByFollowerIdAndFollowingId(userId, viewerId).defaultIfEmpty(false),
                socialMediaRepository.findByUserId(userId).map(this::toSocialMediaSnapshot).collectList()
        ).map(tuple -> new ProfileRelationshipSnapshot(
                tuple.getT1(),
                tuple.getT2(),
                tuple.getT3(),
                Boolean.TRUE.equals(tuple.getT4()),
                Boolean.TRUE.equals(tuple.getT5()),
                tuple.getT6()
        ));
    }

    public Flux<ConnectionSnapshot> getConnectionRows(String userId, String tab, int limit) {
        String normalizedTab = tab == null ? "FOLLOWERS" : tab.trim().toUpperCase(Locale.ROOT);
        Flux<UserFollower> source = switch (normalizedTab) {
            case "FOLLOWING" -> followerRepository.findFollowingByUserId(userId, limit, 0);
            case "FRIENDS" -> followerRepository.findFriendsByUserId(userId, limit, 0);
            default -> followerRepository.findFollowersByUserId(userId, limit, 0);
        };
        return source.map(row -> new ConnectionSnapshot(
                row.getId(), row.getFollowerId(), row.getFollowingId(), row.getCreatedAt()));
    }

    public Mono<Boolean> isFollowing(String followerId, String followingId) {
        return followerRepository.existsByFollowerIdAndFollowingId(followerId, followingId)
                .defaultIfEmpty(false);
    }

    private UserDetailsSnapshot toUserSnapshot(UserDetails details) {
        return new UserDetailsSnapshot(
                details.getUserId(),
                details.getUsername(),
                details.getFullName(),
                details.getDob(),
                details.getHometown(),
                details.getLivingIn(),
                details.getSex(),
                details.getHobbyList()
        );
    }

    private MediaSnapshot toMediaSnapshot(Media media) {
        return new MediaSnapshot(
                media.getAssetId(),
                media.getPublicId(),
                media.getWidth(),
                media.getHeight(),
                media.getMediaFormat(),
                media.getResourceType(),
                media.getBytes(),
                media.getUrl(),
                media.getSecureUrl(),
                media.getOwnerId(),
                media.getOwnerType() == null ? null : media.getOwnerType().name(),
                media.getVersion(),
                media.getVersionId(),
                media.getDisplayName(),
                media.getCreatedAt(),
                media.getUpdatedAt()
        );
    }

    private SocialMediaSnapshot toSocialMediaSnapshot(UserSocialMedia value) {
        return new SocialMediaSnapshot(value.getId(), value.getUserId(), value.getLink());
    }

    private JobSnapshot toJobSnapshot(UserJob value) {
        return new JobSnapshot(
                value.getId(), value.getUserId(), value.getCompanyName(), value.getPosition(),
                value.getFromDate(), value.getToDate(), value.isPublic());
    }

    private UniversitySnapshot toUniversitySnapshot(UserUniversity value) {
        return new UniversitySnapshot(
                value.getId(), value.getUserId(), value.getSchoolName(), value.getMajor(),
                value.getFrom(), value.getTo(), value.isGraduate(), value.isPublic());
    }

    private HighSchoolSnapshot toHighSchoolSnapshot(UserHighSchool value) {
        return new HighSchoolSnapshot(
                value.getId(), value.getUserId(), value.getSchoolName(),
                value.getFromDate(), value.getToDate(), value.isGraduate(), value.isPublic());
    }

    public record ProfileBundleSnapshot(
            UserDetailsSnapshot user,
            MediaSnapshot currentAvatar,
            ProfileRelationshipSnapshot relationship,
            List<JobSnapshot> jobs,
            List<UniversitySnapshot> universities,
            List<HighSchoolSnapshot> highSchools
    ) {
    }

    public record UserDetailsSnapshot(
            String userId,
            String username,
            String fullName,
            LocalDate dob,
            String hometown,
            String livingIn,
            String sex,
            List<String> hobbyList
    ) {
        public UserDetailsSnapshot {
            hobbyList = hobbyList == null ? List.of() : List.copyOf(hobbyList);
        }
    }

    public record MediaSnapshot(
            String assetId,
            String publicId,
            int width,
            int height,
            String mediaFormat,
            String resourceType,
            int bytes,
            String url,
            String secureUrl,
            String ownerId,
            String ownerType,
            String version,
            String versionId,
            String displayName,
            Instant createdAt,
            Instant updatedAt
    ) {
    }

    public record SocialMediaSnapshot(String id, String userId, String link) {
    }

    public record JobSnapshot(
            String id,
            String userId,
            String companyName,
            String position,
            LocalDate fromDate,
            LocalDate toDate,
            @JsonProperty("public") boolean visibleToPublic
    ) {
    }

    public record UniversitySnapshot(
            String id,
            String userId,
            String schoolName,
            String major,
            LocalDate from,
            LocalDate to,
            @JsonProperty("graduate") boolean graduate,
            @JsonProperty("public") boolean visibleToPublic
    ) {
    }

    public record HighSchoolSnapshot(
            String id,
            String userId,
            String schoolName,
            LocalDate fromDate,
            LocalDate toDate,
            @JsonProperty("graduate") boolean graduate,
            @JsonProperty("public") boolean visibleToPublic
    ) {
    }

    public record ConnectionSnapshot(
            String id,
            String followerId,
            String followingId,
            Instant createdAt
    ) {
    }

    public record ProfileRelationshipSnapshot(
            long followerCount,
            long followingCount,
            long friendCount,
            boolean viewerFollowsUser,
            boolean userFollowsViewer,
            List<SocialMediaSnapshot> socialMedia
    ) {
        public ProfileRelationshipSnapshot {
            socialMedia = socialMedia == null ? List.of() : List.copyOf(socialMedia);
        }
    }
}
