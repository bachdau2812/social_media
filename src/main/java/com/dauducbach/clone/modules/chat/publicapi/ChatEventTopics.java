package com.dauducbach.clone.modules.chat.publicapi;

/** Kafka topic names shared by chat publishers and in-process consumers. */
public final class ChatEventTopics {
    public static final String MESSAGE_CREATED = "chat.message.created";
    public static final String MESSAGE_REACTION_CHANGED = "chat.message.reaction.changed";
    public static final String MESSAGE_MUTATION = "chat.message.mutation";
    public static final String CURSOR_UPDATED = "chat.cursor.updated";
    public static final String MEMBER_REQUESTED = "chat.member.requested";
    public static final String MEMBERSHIP_CHANGED = "chat.membership.changed";

    private ChatEventTopics() {
    }
}
