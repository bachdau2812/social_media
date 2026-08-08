package com.dauducbach.clone.modules.media.dto.music.response;

public record JamendoMusicImportResponse(
        int totalReceived,
        int savedCount,
        int skippedCount,
        int failedCount,
        String message
) {
}
