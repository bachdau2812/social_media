package com.dauducbach.clone.modules.frontend.service;

import com.dauducbach.clone.modules.feed.publicapi.FeedScreenQuery;
import com.dauducbach.clone.modules.feed.publicapi.FeedScreenQuery.FeedSnapshot;
import com.dauducbach.clone.modules.frontend.dto.HomeScreenResponse;
import com.dauducbach.clone.modules.frontend.dto.StoryTrayItemResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.publicapi.StoryTrayQuery;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class HomeScreenService {
    FeedScreenQuery feedQuery;
    StoryTrayQuery storyTrayQuery;

    public Mono<HomeScreenResponse> getHome(String userId, String tab, int limit, int page,
            MediaDisplayType mediaType, String cursor) {
        String activeTab = tab == null || tab.isBlank() ? "DISCOVER" : tab.trim().toUpperCase();
        int safeLimit = limit <= 0 ? 20 : Math.min(limit, 50);
        int safePage = Math.max(0, page);
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.FEED : mediaType;
        Mono<FeedSnapshot> feed = "FRIENDS".equals(activeTab)
                ? feedQuery.getFriendsFeed(userId, safeLimit, safePage, displayType)
                : feedQuery.getDiscoverFeed(userId, safeLimit, displayType, cursor);

        return Mono.zip(storyTrayQuery.getHomeStoryTray(userId), feed)
                .map(tuple -> new HomeScreenResponse(
                        activeTab,
                        List.of(
                                new HomeScreenResponse.HomeTab("DISCOVER", "Kham pha", 0),
                                new HomeScreenResponse.HomeTab("FRIENDS", "Ban be", 0)
                        ),
                        tuple.getT1().stream().map(this::toStoryTrayItem).toList(),
                        tuple.getT2(),
                        List.of()
                ));
    }

    private StoryTrayItemResponse toStoryTrayItem(StoryTrayQuery.StoryTraySnapshot story) {
        return new StoryTrayItemResponse(
                story.storyId(),
                story.userId(),
                story.username(),
                story.fullName(),
                story.avatarUrl(),
                story.mediaUrl(),
                story.mediaType(),
                story.musicId(),
                story.musicUrl(),
                story.musicDisplayName(),
                story.musicStart(),
                story.musicEnd(),
                story.durationSeconds(),
                story.status(),
                story.createdAt(),
                story.expiredAt(),
                story.publicationId(),
                story.publicationOrder(),
                story.publicationItemCount(),
                story.viewerSeen(),
                story.viewerReaction()
        );
    }
}
