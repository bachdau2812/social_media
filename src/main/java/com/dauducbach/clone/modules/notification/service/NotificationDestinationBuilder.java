package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;
import java.util.Set;

@Component
public class NotificationDestinationBuilder {
    private static final Set<UserActionType> POST_ACTIONS = Set.of(
            UserActionType.NEW_POST,
            UserActionType.LIKE,
            UserActionType.LIKES,
            UserActionType.LIKE_OTHER_INTERACT_PEOPLE);
    private static final Set<UserActionType> COMMENT_ACTIONS = Set.of(
            UserActionType.COMMENT,
            UserActionType.COMMENTS,
            UserActionType.COMMENT_OTHER_INTERACT_PEOPLE,
            UserActionType.REPLY_COMMENT,
            UserActionType.LIKE_COMMENT);

    public String build(NotificationForService notification) {
        if (notification == null || notification.getActionType() == null) {
            return "/";
        }

        UserActionType actionType = notification.getActionType();
        Map<String, String> metadata = notification.getMetadata() == null
                ? Map.of()
                : notification.getMetadata();

        if (actionType == UserActionType.SEND_MESSAGE) {
            return messageDestination(notification, metadata);
        }
        if (actionType == UserActionType.CHAT_MEMBER_REQUEST) {
            return groupDestination(notification, metadata, true);
        }
        if (actionType == UserActionType.CHAT_GROUP_MEMBER_ADDED) {
            return groupDestination(notification, metadata, false);
        }
        if (actionType == UserActionType.FOLLOW_EVENT
                || actionType == UserActionType.STORY_ACTIVITY) {
            return profileDestination(firstNonBlank(notification.getActorId(), metadataValue(metadata, "USER_ID")));
        }
        if (actionType == UserActionType.UP_STORY
                || actionType == UserActionType.LIKE_STORY) {
            return storyDestination(notification, metadata, actionType == UserActionType.LIKE_STORY);
        }
        if (COMMENT_ACTIONS.contains(actionType)) {
            return commentDestination(notification, metadata);
        }
        if (POST_ACTIONS.contains(actionType)) {
            return postDestination(firstNonBlank(metadataValue(metadata, "POST_ID"), notification.getEntityId()));
        }

        return fallbackDestination(notification, metadata);
    }

    private String messageDestination(NotificationForService notification, Map<String, String> metadata) {
        String conversationId = metadataValue(metadata, "CONVERSATION_ID");
        if (conversationId.isBlank()) {
            return "/";
        }
        UriComponentsBuilder builder = UriComponentsBuilder.fromPath("/messages")
                .queryParam("conversationId", conversationId);
        addQueryParam(builder, "messageId",
                firstNonBlank(metadataValue(metadata, "MESSAGE_ID"), notification.getEntityId()));
        addQueryParam(builder, "messageSeq", metadataValue(metadata, "MESSAGE_SEQ"));
        return builder.build().encode().toUriString();
    }

    private String groupDestination(
            NotificationForService notification,
            Map<String, String> metadata,
            boolean openRequests
    ) {
        String conversationId = firstNonBlank(
                metadataValue(metadata, "CONVERSATION_ID"),
                notification.getEntityType() != null
                        && notification.getEntityType().toUpperCase().contains("CONVERSATION")
                        ? notification.getEntityId()
                        : "");
        if (conversationId.isBlank()) {
            return "/";
        }
        UriComponentsBuilder builder = UriComponentsBuilder.fromPath("/messages")
                .queryParam("conversationId", conversationId);
        if (openRequests) {
            builder.queryParam("panel", "requests");
        }
        return builder.build().encode().toUriString();
    }

    private String commentDestination(NotificationForService notification, Map<String, String> metadata) {
        String postId = metadataValue(metadata, "POST_ID");
        if (postId.isBlank()) {
            return "/";
        }
        String commentId = firstNonBlank(
                metadataValue(metadata, "COMMENT_ID"),
                notification.getEntityId());
        UriComponentsBuilder builder = UriComponentsBuilder.fromPath("/posts/{postId}")
                .queryParam("commentId", commentId);
        return builder.buildAndExpand(postId).encode().toUriString();
    }

    private String storyDestination(
            NotificationForService notification,
            Map<String, String> metadata,
            boolean singleStory
    ) {
        String ownerId = firstNonBlank(
                metadataValue(metadata, "STORY_OWNER_ID"),
                metadataValue(metadata, "USER_ID"),
                notification.getActorId());
        String storyId = firstNonBlank(metadataValue(metadata, "STORY_ID"), notification.getEntityId());
        if (ownerId.isBlank()) {
            return "/";
        }
        UriComponentsBuilder builder = UriComponentsBuilder.fromPath("/stories")
                .queryParam("ownerId", ownerId);
        addQueryParam(builder, "storyId", storyId);
        if (singleStory) {
            builder.queryParam("storyScope", "single");
        } else {
            addQueryParam(builder, "storyItemId", metadataValue(metadata, "STORY_ITEM_ID"));
            builder.queryParam("scoped", "true");
        }
        return builder.build().encode().toUriString();
    }

    private String fallbackDestination(NotificationForService notification, Map<String, String> metadata) {
        String entityType = notification.getEntityType() == null
                ? ""
                : notification.getEntityType().trim().toUpperCase();
        return switch (entityType) {
            case "POST" -> postDestination(notification.getEntityId());
            case "USER" -> profileDestination(notification.getEntityId());
            case "STORY" -> storyDestination(notification, metadata, false);
            case "CHAT_CONVERSATION" -> groupDestination(notification, metadata, false);
            default -> "/";
        };
    }

    private String postDestination(String postId) {
        return postId == null || postId.isBlank()
                ? "/"
                : UriComponentsBuilder.fromPath("/posts/{postId}")
                .buildAndExpand(postId)
                .encode()
                .toUriString();
    }

    private String profileDestination(String userId) {
        return userId == null || userId.isBlank()
                ? "/"
                : UriComponentsBuilder.fromPath("/profiles/{userId}")
                .buildAndExpand(userId)
                .encode()
                .toUriString();
    }

    private void addQueryParam(UriComponentsBuilder builder, String name, String value) {
        if (value != null && !value.isBlank()) {
            builder.queryParam(name, value);
        }
    }

    private String metadataValue(Map<String, String> metadata, String key) {
        String exact = metadata.get(key);
        if (exact != null) {
            return exact.trim();
        }
        return metadata.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getKey().equalsIgnoreCase(key))
                .map(Map.Entry::getValue)
                .filter(value -> value != null)
                .map(String::trim)
                .findFirst()
                .orElse("");
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }
}
