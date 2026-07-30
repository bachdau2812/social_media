package com.dauducbach.clone.modules.feed.dto.response;

import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;

import java.time.Instant;
import java.util.List;

public record FeedItemResponse(
        String postId,
        String userId,
        String authorUsername,
        String authorFullName,
        String authorAvatarUrl,
        String content,
        List<String> hashtags,
        String mediaRatio,
        List<FeedMediaResponse> media,
        PostMusicResponse music,
        List<PostItemResponse> items,
        long likeCount,
        long commentCount,
        long repostCount,
        boolean likedByCurrentUser,
        boolean repostedByCurrentUser,
        Instant createdAt,
        Instant updatedAt,
        String sourceType,
        String recommendationReason,
        String rankingVersion,
        String experimentId,
        String impressionToken
) {
    public FeedItemResponse(
            String postId,
            String userId,
            String authorUsername,
            String authorFullName,
            String authorAvatarUrl,
            String content,
            List<String> hashtags,
            String mediaRatio,
            List<FeedMediaResponse> media,
            PostMusicResponse music,
            List<PostItemResponse> items,
            long likeCount,
            long commentCount,
            long repostCount,
            boolean likedByCurrentUser,
            boolean repostedByCurrentUser,
            Instant createdAt,
            Instant updatedAt
    ) {
        this(
                postId,
                userId,
                authorUsername,
                authorFullName,
                authorAvatarUrl,
                content,
                hashtags,
                mediaRatio,
                media,
                music,
                items,
                likeCount,
                commentCount,
                repostCount,
                likedByCurrentUser,
                repostedByCurrentUser,
                createdAt,
                updatedAt,
                null,
                null,
                null,
                null,
                null
        );
    }
}