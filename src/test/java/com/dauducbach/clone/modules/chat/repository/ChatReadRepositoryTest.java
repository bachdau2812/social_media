package com.dauducbach.clone.modules.chat.repository;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

class ChatReadRepositoryTest {

    private TimeZone originalTimeZone;

    @BeforeEach
    void useAsiaSaigonTimeZone() {
        originalTimeZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Saigon"));
    }

    @AfterEach
    void restoreTimeZone() {
        TimeZone.setDefault(originalTimeZone);
    }

    @Test
    void mapsLocalDateTimeUsingConfiguredSystemZone() {
        LocalDateTime databaseValue = LocalDateTime.of(2026, 7, 24, 0, 0);
        Instant expected = databaseValue.atZone(ZoneId.systemDefault()).toInstant();

        assertThat(ChatReadRepository.toInstant(databaseValue)).isEqualTo(expected);
        assertThat(expected).isEqualTo(Instant.parse("2026-07-23T17:00:00Z"));
    }
}
