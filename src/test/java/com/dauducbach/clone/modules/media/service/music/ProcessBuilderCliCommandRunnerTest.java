package com.dauducbach.clone.modules.media.service.music;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessBuilderCliCommandRunnerTest {
    private final Scheduler scheduler = Schedulers.newBoundedElastic(1, 10, "cli-test");

    @AfterEach
    void disposeScheduler() {
        scheduler.dispose();
    }

    @Test
    void runsCommandDirectlyAndCapturesMergedOutput() {
        ProcessBuilderCliCommandRunner runner = new ProcessBuilderCliCommandRunner(scheduler);

        StepVerifier.create(runner.run(List.of("java", "-version"), Duration.ofSeconds(20)))
                .assertNext(result -> {
                    assertThat(result.exitCode()).isZero();
                    assertThat(result.output()).containsIgnoringCase("version");
                })
                .verifyComplete();
    }

    @Test
    void rejectsAnEmptyCommand() {
        ProcessBuilderCliCommandRunner runner = new ProcessBuilderCliCommandRunner(scheduler);

        StepVerifier.create(runner.run(List.of(), Duration.ofSeconds(1)))
                .expectError(IllegalArgumentException.class)
                .verify();
    }
}
