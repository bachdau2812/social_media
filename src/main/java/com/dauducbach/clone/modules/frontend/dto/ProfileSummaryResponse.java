package com.dauducbach.clone.modules.frontend.dto;

import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.HighSchoolSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.JobSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.MediaSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.SocialMediaSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.UniversitySnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.UserDetailsSnapshot;

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
