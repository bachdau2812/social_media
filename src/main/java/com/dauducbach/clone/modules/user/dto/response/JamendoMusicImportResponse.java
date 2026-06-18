package com.dauducbach.clone.modules.user.dto.response;

public record JamendoMusicImportResponse(
        int totalReceived,
        int savedCount,
        int skippedCount,
        int failedCount,
        String message
) {
}
