package com.dauducbach.clone.modules.user.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

public class FollowerListResponse {
    List<FollowerInfo> followers;
    int totalCount;
    int currentPage;
    int pageSize;
    boolean hasNextPage;
    boolean hasPreviousPage;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @FieldDefaults(level = lombok.AccessLevel.PRIVATE)
    @Builder

    public static class FollowerInfo {
        String followId;      // ID của follow relationship
        String userId;        // ID của user (follower hoặc following)
        String followedAt;    // Thời gian follow (ISO format)
    }
}