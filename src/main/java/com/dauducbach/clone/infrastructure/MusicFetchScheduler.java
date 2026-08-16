package com.dauducbach.clone.infrastructure;

import com.dauducbach.clone.modules.media.dto.music.request.BulkMusicFetchRequest;
import com.dauducbach.clone.modules.media.service.music.BulkMusicFetchService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class MusicFetchScheduler {
    private static final Logger log = LoggerFactory.getLogger(MusicFetchScheduler.class);
    private static final int DAILY_TOP_LIMIT = 20;

    private final BulkMusicFetchService bulkMusicFetchService;

    @Scheduled(cron = "0 0 2 * * *", zone = "Asia/Ho_Chi_Minh")
    public void fetchTopUnfetchedMusics() {
        log.info("|MusicFetchScheduler|fetchTopUnfetchedMusics|start|limit={}", DAILY_TOP_LIMIT);
        bulkMusicFetchService.triggerFetch(BulkMusicFetchRequest.top(DAILY_TOP_LIMIT))
                .subscribe(
                        response -> log.info(
                                "|MusicFetchScheduler|fetchTopUnfetchedMusics|completed|selected={}|started={}|processing={}|alreadyFetched={}|failed={}",
                                response.selectedCount(),
                                response.startedCount(),
                                response.processingCount(),
                                response.alreadyFetchedCount(),
                                response.failedCount()),
                        error -> log.error(
                                "|MusicFetchScheduler|fetchTopUnfetchedMusics|failed|errorType={}|error={}",
                                error.getClass().getSimpleName(),
                                error.getMessage()));
    }
}
