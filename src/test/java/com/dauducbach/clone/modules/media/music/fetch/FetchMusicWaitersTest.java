package com.dauducbach.clone.modules.media.music.fetch;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FetchMusicWaitersTest {
    @Test
    void tracksDistinctUsersAndReplaysTerminalResultToLateWaiters() {
        FetchMusicWaiters waiters = new FetchMusicWaiters();
        var first = waiters.register("track-1", "user-1");
        var duplicate = waiters.register("track-1", "user-1");
        waiters.register("track-1", "user-2");

        var terminal = new FetchMusicWaiters.Terminal("music_fetch_success", "payload");
        assertThat(waiters.state("track-1").complete(terminal))
                .containsExactlyInAnyOrder("user-1", "user-2");

        var late = waiters.register("track-1", "user-3");
        assertThat(late.terminal()).isEqualTo(terminal);
        assertThat(waiters.state("track-1").complete(terminal)).isEmpty();

        waiters.unregister("track-1", first);
        waiters.unregister("track-1", duplicate);
        waiters.retire("track-1", first.state());
        assertThat(waiters.state("track-1")).isNotSameAs(first.state());
    }

    @Test
    void removesAnUnfinishedStateAfterItsLastWaiterCancels() {
        FetchMusicWaiters waiters = new FetchMusicWaiters();
        var registration = waiters.register("track-2", "user-1");

        waiters.unregister("track-2", registration);

        assertThat(waiters.state("track-2")).isNotSameAs(registration.state());
    }
}
