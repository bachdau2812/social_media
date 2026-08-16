package com.dauducbach.clone.infrastructure;

import com.dauducbach.clone.modules.media.constant.MusicFetchType;
import com.dauducbach.clone.modules.media.dto.music.request.BulkMusicFetchRequest;
import com.dauducbach.clone.modules.media.dto.music.response.BulkMusicFetchResponse;
import com.dauducbach.clone.modules.media.service.music.BulkMusicFetchService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MusicFetchSchedulerTest {

    @Test
    void declaresDailyTwoAmScheduleInHoChiMinhTime() throws Exception {
        Method method = MusicFetchScheduler.class.getDeclaredMethod("fetchTopUnfetchedMusics");
        Scheduled scheduled = method.getAnnotation(Scheduled.class);

        assertThat(scheduled).isNotNull();
        assertThat(scheduled.cron()).isEqualTo("0 0 2 * * *");
        assertThat(scheduled.zone()).isEqualTo("Asia/Ho_Chi_Minh");
    }

    @Test
    void triggersTopTwentyWithoutFetchList() {
        BulkMusicFetchService service = mock(BulkMusicFetchService.class);
        when(service.triggerFetch(org.mockito.ArgumentMatchers.any()))
                .thenReturn(Mono.just(BulkMusicFetchResponse.from(MusicFetchType.TOP, List.of())));

        new MusicFetchScheduler(service).fetchTopUnfetchedMusics();

        ArgumentCaptor<BulkMusicFetchRequest> request = ArgumentCaptor.forClass(BulkMusicFetchRequest.class);
        verify(service).triggerFetch(request.capture());
        assertThat(request.getValue().type()).isEqualTo(MusicFetchType.TOP);
        assertThat(request.getValue().fetchList()).isEmpty();
        assertThat(request.getValue().limit()).isEqualTo(20);
    }

    @Test
    void containsReactiveFailureInsideScheduledInvocation() {
        BulkMusicFetchService service = mock(BulkMusicFetchService.class);
        when(service.triggerFetch(org.mockito.ArgumentMatchers.any()))
                .thenReturn(Mono.error(new IllegalStateException("database unavailable")));

        assertThatCode(() -> new MusicFetchScheduler(service).fetchTopUnfetchedMusics())
                .doesNotThrowAnyException();
    }
}
