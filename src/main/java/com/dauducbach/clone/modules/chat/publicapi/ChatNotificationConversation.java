package com.dauducbach.clone.modules.chat.publicapi;

/** Conversation presentation data for message notifications, without persistence details. */
public record ChatNotificationConversation(boolean group, String title) {
}
