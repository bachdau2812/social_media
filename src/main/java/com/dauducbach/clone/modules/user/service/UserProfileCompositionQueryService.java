package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import com.dauducbach.clone.modules.user.entity.UserJob;
import com.dauducbach.clone.modules.user.entity.UserSocialMedia;
import com.dauducbach.clone.modules.user.entity.UserUniversity;
import com.dauducbach.clone.modules.user.repository.UserSocialMediaRepository;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.ConnectionSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.HighSchoolSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.JobSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.MediaSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.ProfileBundleSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.ProfileContentSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.ProfileRelationshipSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.SocialMediaSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.UniversitySnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.UserDetailsSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserRelationshipQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class UserProfileCompositionQueryService implements UserProfileQuery {
    private final UserRelationshipQuery relationshipQuery;
    private final UserSocialMediaRepository socialMediaRepository;
    private final UserDetailsService userDetailsService;
    private final UserJobService userJobService;
    private final UserUniversityService userUniversityService;
    private final UserHighSchoolService userHighSchoolService;
    private final MediaCatalog mediaCatalog;

    @Override
    public Mono<ProfileBundleSnapshot> getProfileBundle(String viewerId, String userId, boolean isOwner) {
        Mono<Optional<MediaSnapshot>> avatar = mediaCatalog.findCurrentAvatar(userId)
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

    @Override
    public Mono<ProfileContentSnapshot> getProfileContentSnapshot(String userId) {
        return Mono.zip(
                userDetailsService.getUserDetailsById(userId).map(this::toUserSnapshot),
                userJobService.getUserJobsByUserId(userId, true).map(this::toJobSnapshot).collectList(),
                userUniversityService.getUserUniversitiesByUserId(userId, true).map(this::toUniversitySnapshot).collectList(),
                userHighSchoolService.getUserHighSchoolsByUserId(userId, true).map(this::toHighSchoolSnapshot).collectList()
        ).map(tuple -> new ProfileContentSnapshot(tuple.getT1(), tuple.getT2(), tuple.getT3(), tuple.getT4()));
    }

    @Override
    public Mono<ProfileRelationshipSnapshot> getRelationshipSnapshot(String viewerId, String userId) {
        return Mono.zip(
                relationshipQuery.getRelationshipSummary(viewerId, userId),
                socialMediaRepository.findByUserId(userId).map(this::toSocialMediaSnapshot).collectList()
        ).map(tuple -> new ProfileRelationshipSnapshot(
                tuple.getT1().followerCount(),
                tuple.getT1().followingCount(),
                tuple.getT1().friendCount(),
                tuple.getT1().viewerFollowsUser(),
                tuple.getT1().userFollowsViewer(),
                tuple.getT2()
        ));
    }

    @Override
    public Flux<ConnectionSnapshot> getConnectionRows(String userId, String tab, int limit) {
        return relationshipQuery.getConnections(userId, tab, limit)
                .map(row -> new ConnectionSnapshot(row.id(), row.followerId(), row.followingId(), row.createdAt()));
    }

    @Override
    public Mono<Boolean> isFollowing(String followerId, String followingId) {
        return relationshipQuery.isFollowing(followerId, followingId);
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

    private MediaSnapshot toMediaSnapshot(MediaAssetView media) {
        return new MediaSnapshot(
                media.assetId(),
                media.publicId(),
                media.width(),
                media.height(),
                media.mediaFormat(),
                media.resourceType(),
                media.bytes(),
                media.url(),
                media.secureUrl(),
                media.ownerId(),
                media.ownerType() == null ? null : media.ownerType().name(),
                media.version(),
                media.versionId(),
                media.displayName(),
                media.createdAt(),
                media.updatedAt()
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

}
