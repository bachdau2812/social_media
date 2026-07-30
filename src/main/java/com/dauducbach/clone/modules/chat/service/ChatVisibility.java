package com.dauducbach.clone.modules.chat.service;

public final class ChatVisibility {

    private ChatVisibility() {
    }

    public static long visibleFromSequence(long joinedSequence, Long lastDeletedMessageSequence) {
        return lastDeletedMessageSequence == null
                ? joinedSequence
                : Math.max(joinedSequence, lastDeletedMessageSequence + 1L);
    }

    public static boolean isHiddenFromList(long lastMessageSequence, Long lastDeletedMessageSequence) {
        return lastDeletedMessageSequence != null && lastMessageSequence <= lastDeletedMessageSequence;
    }
}