package com.dauducbach.clone.modules.media.dto.music.request;

import com.dauducbach.clone.modules.media.constant.MusicFetchType;
import com.dauducbach.clone.modules.media.dto.music.response.BulkMusicFetchItemResponse;
import com.dauducbach.clone.modules.media.dto.music.response.BulkMusicFetchResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BulkMusicFetchRequestTest {

    @Test
    void createsTopRequestWithRequestedLimit() {
        BulkMusicFetchRequest request = BulkMusicFetchRequest.top(20);

        assertThat(request.type()).isEqualTo(MusicFetchType.TOP);
        assertThat(request.fetchList()).isEmpty();
        assertThat(request.limit()).isEqualTo(20);
    }

    @Test
    void summarizesEveryBulkFetchStatus() {
        List<BulkMusicFetchItemResponse> items = List.of(
                new BulkMusicFetchItemResponse("track-started", BulkMusicFetchItemResponse.Status.STARTED, null),
                new BulkMusicFetchItemResponse("track-processing", BulkMusicFetchItemResponse.Status.PROCESSING, null),
                new BulkMusicFetchItemResponse("track-fetched", BulkMusicFetchItemResponse.Status.ALREADY_FETCHED, null),
                new BulkMusicFetchItemResponse("track-failed", BulkMusicFetchItemResponse.Status.FAILED, "Music fetch failed")
        );

        BulkMusicFetchResponse response = BulkMusicFetchResponse.from(MusicFetchType.SONG, items);

        assertThat(response.type()).isEqualTo(MusicFetchType.SONG);
        assertThat(response.selectedCount()).isEqualTo(4);
        assertThat(response.startedCount()).isEqualTo(1);
        assertThat(response.processingCount()).isEqualTo(1);
        assertThat(response.alreadyFetchedCount()).isEqualTo(1);
        assertThat(response.failedCount()).isEqualTo(1);
        assertThat(response.items()).containsExactlyElementsOf(items);
    }
}
