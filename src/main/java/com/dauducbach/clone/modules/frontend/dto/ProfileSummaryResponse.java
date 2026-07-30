package com.dauducbach.clone.modules.frontend.dto;

import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.HighSchoolSnapshot;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.JobSnapshot;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.MediaSnapshot;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.SocialMediaSnapshot;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.UniversitySnapshot;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.UserDetailsSnapshot;

import java.util.List;

public record ProfileSummaryResponse(
        UserDetailsSnapshot user,
        MediaSnapshot currentAvatar,
        long followerCount,
        long followingCount,
        long friendCount,
        boolean viewerFollowsUser,
        boolean userFollowsViewer,
        boolean friend,
        List<SocialMediaSnapshot> socialMedia,
        List<JobSnapshot> jobs,
        List<UniversitySnapshot> universities,
        List<HighSchoolSnapshot> highSchools,
        List<ProfilePostResponse> recentPosts,
        List<ProfilePostResponse> repostedPosts
) {
}
