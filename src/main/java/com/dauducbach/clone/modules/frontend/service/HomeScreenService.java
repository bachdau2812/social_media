package com.dauducbach.clone.modules.frontend.service;

import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.feed.service.FeedService;
import com.dauducbach.clone.modules.frontend.dto.HomeScreenResponse;
import com.dauducbach.clone.modules.frontend.dto.StoryTrayItemResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.user.dto.response.StoryTrayResponse;
import com.dauducbach.clone.modules.user.service.StoryTrayQueryService;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class HomeScreenService {
    FeedService feedService;
    StoryTrayQueryService storyTrayQueryService;

    public Mono<HomeScreenResponse> getHome(
            String userId,
            String tab,
            int limit,
            int page,
            MediaDisplayType mediaType
    ) {
        String activeTab = tab == null || tab.isBlank() ? "DISCOVER" : tab.trim().toUpperCase();
        int safeLimit = limit <= 0 ? 20 : Math.min(limit, 50);
        int safePage = Math.max(0, page);
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.FEED : mediaType;
        Mono<FeedResponse> feed = "FRIENDS".equals(activeTab)
                ? feedService.getFriendsFeed(userId, safeLimit, safePage, displayType)
                : feedService.getFeed(userId, safeLimit, displayType);

        return Mono.zip(storyTrayQueryService.getHomeStoryTray(userId), feed)
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

    private StoryTrayItemResponse toStoryTrayItem(StoryTrayResponse story) {
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
                story.viewerSeen()
        );
    }
}