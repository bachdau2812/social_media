package com.dauducbach.clone.modules.media.service.music;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Path;
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

    @Test
    void forcesUtf8ForPythonBasedCliProcesses() {
        ProcessBuilderCliCommandRunner runner = new ProcessBuilderCliCommandRunner(scheduler);
        String javaCommand = Path.of(
                        System.getProperty("java.home"),
                        "bin",
                        System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java")
                .toString();

        StepVerifier.create(runner.run(List.of(
                        javaCommand,
                        "-cp",
                        System.getProperty("java.class.path"),
                        EnvironmentProbe.class.getName()), Duration.ofSeconds(20)))
                .assertNext(result -> {
                    assertThat(result.exitCode()).isZero();
                    assertThat(result.output()).contains("PYTHONUTF8=1");
                    assertThat(result.output()).contains("PYTHONIOENCODING=utf-8");
                })
                .verifyComplete();
    }

    @Test
    void closesStandardInputForNonInteractiveCliProcesses() {
        ProcessBuilderCliCommandRunner runner = new ProcessBuilderCliCommandRunner(scheduler);
        String javaCommand = Path.of(
                        System.getProperty("java.home"),
                        "bin",
                        System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java")
                .toString();

        StepVerifier.create(runner.run(List.of(
                        javaCommand,
                        "-cp",
                        System.getProperty("java.class.path"),
                        StdinProbe.class.getName()), Duration.ofSeconds(2)))
                .assertNext(result -> {
                    assertThat(result.exitCode()).isZero();
                    assertThat(result.output()).contains("STDIN_EOF");
                })
                .verifyComplete();
    }

    public static final class StdinProbe {
        public static void main(String[] args) throws Exception {
            System.out.println(System.in.read() == -1 ? "STDIN_EOF" : "STDIN_OPEN");
        }
    }

    public static final class EnvironmentProbe {
        public static void main(String[] args) {
            System.out.println("PYTHONUTF8=" + System.getenv("PYTHONUTF8"));
            System.out.println("PYTHONIOENCODING=" + System.getenv("PYTHONIOENCODING"));
        }
    }
}
