package com.dauducbach.clone.modules.frontend.dto;

import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import com.dauducbach.clone.modules.user.entity.UserJob;
import com.dauducbach.clone.modules.user.entity.UserUniversity;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.HighSchoolSnapshot;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.JobSnapshot;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.MediaSnapshot;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.UniversitySnapshot;
import com.dauducbach.clone.modules.user.service.UserProfileCompositionQueryService.UserDetailsSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProfileSummaryResponseContractTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void snapshotsPreserveExistingNestedJsonShape() {
        UserDetails user = UserDetails.builder()
                .userId("u1")
                .username("alice")
                .fullName("Alice")
                .hobbyList("[]")
                .build();
        UserDetailsSnapshot userSnapshot =
                new UserDetailsSnapshot("u1", "alice", "Alice", null, null, null, null, List.of());
        assertSameJson(user, userSnapshot);

        Media media = Media.builder()
                .assetId("asset")
                .publicId("public")
                .width(100)
                .height(200)
                .mediaFormat("jpg")
                .resourceType("image")
                .bytes(42)
                .url("http://media")
                .secureUrl("https://media")
                .ownerId("u1")
                .ownerType(OwnerType.AVATAR)
                .version("1")
                .versionId("v1")
                .displayName("avatar")
                .build();
        MediaSnapshot mediaSnapshot = new MediaSnapshot(
                "asset", "public", 100, 200, "jpg", "image", 42,
                "http://media", "https://media", "u1", "AVATAR",
                "1", "v1", "avatar", null, null);
        assertSameJson(media, mediaSnapshot);

        UserJob job = UserJob.builder()
                .id("job")
                .userId("u1")
                .companyName("Company")
                .position("Engineer")
                .isPublic(true)
                .build();
        assertSameJson(job, new JobSnapshot(
                "job", "u1", "Company", "Engineer", null, null, true));

        UserUniversity university = UserUniversity.builder()
                .id("uni")
                .userId("u1")
                .schoolName("University")
                .major("CS")
                .isGraduate(true)
                .isPublic(true)
                .build();
        assertSameJson(university, new UniversitySnapshot(
                "uni", "u1", "University", "CS", null, null, true, true));

        UserHighSchool highSchool = UserHighSchool.builder()
                .id("school")
                .userId("u1")
                .schoolName("High School")
                .isGraduate(true)
                .isPublic(true)
                .build();
        assertSameJson(highSchool, new HighSchoolSnapshot(
                "school", "u1", "High School", null, null, true, true));
    }

    private void assertSameJson(Object existingValue, Object snapshot) {
        Map<?, ?> existing = objectMapper.convertValue(existingValue, Map.class);
        Map<?, ?> replacement = objectMapper.convertValue(snapshot, Map.class);
        assertEquals(existing, replacement);
    }
}
