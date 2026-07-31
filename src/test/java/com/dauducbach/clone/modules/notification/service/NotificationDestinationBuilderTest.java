package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationDestinationBuilderTest {
    private final NotificationDestinationBuilder builder = new NotificationDestinationBuilder();

    @Test
    void buildsMessageDestinationWithConversationAndMessage() {
        NotificationForService notification = NotificationForService.builder()
                .actionType(UserActionType.SEND_MESSAGE)
                .entityId("message-1")
                .metadata(Map.of("CONVERSATION_ID", "conversation-1", "MESSAGE_SEQ", "42"))
                .build();

        assertThat(builder.build(notification))
                .isEqualTo("/messages?conversationId=conversation-1&messageId=message-1&messageSeq=42");
    }

    @Test
    void buildsCommentDestinationWithPostAndComment() {
        NotificationForService notification = NotificationForService.builder()
                .actionType(UserActionType.COMMENT)
                .entityId("comment-1")
                .metadata(Map.of("POST_ID", "post-1"))
                .build();

        assertThat(builder.build(notification)).isEqualTo("/posts/post-1?commentId=comment-1");
    }

    @Test
    void buildsScopedStoryDestinationSeparatelyFromStoryActivity() {
        NotificationForService directStory = NotificationForService.builder()
                .actorId("user-1")
                .actionType(UserActionType.UP_STORY)
                .entityId("story-1")
                .build();
        NotificationForService storyActivity = NotificationForService.builder()
                .actorId("user-1")
                .actionType(UserActionType.STORY_ACTIVITY)
                .entityId("story-1")
                .build();

        assertThat(builder.build(directStory))
                .isEqualTo("/stories?ownerId=user-1&storyId=story-1&scoped=true");
        assertThat(builder.build(storyActivity)).isEqualTo("/profiles/user-1");
    }

    @Test
    void buildsSingleStoryDestinationForStoryLikesWithoutChangingUpStoryLinks() {
        NotificationForService storyLike = NotificationForService.builder()
                .actorId("actor-1")
                .actionType(UserActionType.LIKE_STORY)
                .entityId("story-1")
                .entityType("STORY")
                .metadata(Map.of(
                        "STORY_OWNER_ID", "owner-1",
                        "STORY_ID", "story-1",
                        "INTERACTION_ID", "interaction-1"))
                .build();

        assertThat(builder.build(storyLike))
                .isEqualTo("/stories?ownerId=owner-1&storyId=story-1&storyScope=single");
    }

    @Test
    void buildsGroupRequestDestinationWithRequestsPanel() {
        NotificationForService notification = NotificationForService.builder()
                .actionType(UserActionType.CHAT_MEMBER_REQUEST)
                .entityId("request-1")
                .metadata(Map.of("CONVERSATION_ID", "group-1"))
                .build();

        assertThat(builder.build(notification))
                .isEqualTo("/messages?conversationId=group-1&panel=requests");
    }
}
