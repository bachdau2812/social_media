package com.dauducbach.clone.modules.user.publicapi;

import com.fasterxml.jackson.annotation.JsonProperty;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/** Stable read contract for profile screens and profile composition. */
public interface UserProfileQuery {
    Mono<ProfileBundleSnapshot> getProfileBundle(String viewerId, String userId, boolean isOwner);

    /** Current textual profile inputs for consumers that build derived profile projections. */
    Mono<ProfileContentSnapshot> getProfileContentSnapshot(String userId);

    Mono<ProfileRelationshipSnapshot> getRelationshipSnapshot(String viewerId, String userId);

    Flux<ConnectionSnapshot> getConnectionRows(String userId, String tab, int limit);

    Mono<Boolean> isFollowing(String followerId, String followingId);

    record ProfileBundleSnapshot(
            UserDetailsSnapshot user,
            MediaSnapshot currentAvatar,
            ProfileRelationshipSnapshot relationship,
            List<JobSnapshot> jobs,
            List<UniversitySnapshot> universities,
            List<HighSchoolSnapshot> highSchools
    ) {
    }

    record ProfileContentSnapshot(
            UserDetailsSnapshot user,
            List<JobSnapshot> jobs,
            List<UniversitySnapshot> universities,
            List<HighSchoolSnapshot> highSchools
    ) {
        public ProfileContentSnapshot {
            jobs = jobs == null ? List.of() : List.copyOf(jobs);
            universities = universities == null ? List.of() : List.copyOf(universities);
            highSchools = highSchools == null ? List.of() : List.copyOf(highSchools);
        }
    }

    record UserDetailsSnapshot(
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

    record MediaSnapshot(
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

    record SocialMediaSnapshot(String id, String userId, String link) {
    }

    record JobSnapshot(
            String id,
            String userId,
            String companyName,
            String position,
            LocalDate fromDate,
            LocalDate toDate,
            @JsonProperty("public") boolean visibleToPublic
    ) {
    }

    record UniversitySnapshot(
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

    record HighSchoolSnapshot(
            String id,
            String userId,
            String schoolName,
            LocalDate fromDate,
            LocalDate toDate,
            @JsonProperty("graduate") boolean graduate,
            @JsonProperty("public") boolean visibleToPublic
    ) {
    }

    record ConnectionSnapshot(
            String id,
            String followerId,
            String followingId,
            Instant createdAt
    ) {
    }

    record ProfileRelationshipSnapshot(
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
