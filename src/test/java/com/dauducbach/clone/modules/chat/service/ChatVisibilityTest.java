package com.dauducbach.clone.modules.chat.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ChatVisibilityTest {

    @Test
    void visibleHistoryNeverStartsBeforeJoinedSequence() {
        assertThat(ChatVisibility.visibleFromSequence(25L, null)).isEqualTo(25L);
        assertThat(ChatVisibility.visibleFromSequence(25L, 10L)).isEqualTo(25L);
    }

    @Test
    void localDeletionMovesVisibleHistoryAfterDeletedSequence() {
        assertThat(ChatVisibility.visibleFromSequence(5L, 40L)).isEqualTo(41L);
    }

    @Test
    void deletedConversationStaysHiddenUntilANewerMessageExists() {
        assertThat(ChatVisibility.isHiddenFromList(40L, 40L)).isTrue();
        assertThat(ChatVisibility.isHiddenFromList(41L, 40L)).isFalse();
        assertThat(ChatVisibility.isHiddenFromList(0L, null)).isFalse();
    }
}
