package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.testsupport.TestLogCapture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessBuilderCliCommandRunnerTest {
    private final Scheduler scheduler = Schedulers.newBoundedElastic(1, 10, "cli-test");

    @TempDir
    Path tempDirectory;

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

    @Test
    void logsSpotiFlacOutputBeforeTheProcessFinishes() throws Exception {
        ProcessBuilderCliCommandRunner runner = new ProcessBuilderCliCommandRunner(scheduler);
        CliCommandRequest request = new CliCommandRequest(
                "SpotiFLAC",
                "track-1",
                "job-1",
                javaCommand(StreamingOutputProbe.class),
                Duration.ofSeconds(20),
                true);

        try (TestLogCapture capture = TestLogCapture.start(ProcessBuilderCliCommandRunner.class)) {
            CompletableFuture<CliCommandResult> result = runner.run(request).toFuture();

            assertThat(awaitLog(capture, "[SpotiFLAC] FIRST_OUTPUT", Duration.ofSeconds(10)))
                    .isTrue();
            assertThat(result).isNotDone();
            assertThat(result.get(20, TimeUnit.SECONDS).exitCode()).isZero();
            assertThat(capture.messages()).noneMatch(message -> message.contains("very-secret-token"));
            assertThat(capture.messages()).noneMatch(message -> message.contains("private-cookie"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("set-cookie=[REDACTED]"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("Access token acquired: [REDACTED]"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("trackId=track-1")
                    && message.contains("jobId=job-1")
                    && message.contains("command="));
            assertThat(capture.messages()).anyMatch(message -> message.contains("pid="));
            assertThat(capture.messages()).anyMatch(message -> message.contains("process finished")
                    && message.contains("exitCode=0")
                    && message.contains("elapsedMs="));
        }
    }

    private boolean awaitLog(TestLogCapture capture, String marker, Duration timeout)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (capture.messages().stream().anyMatch(message -> message.contains(marker))) {
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }

    private List<String> javaCommand(Class<?> probe) {
        String javaCommand = Path.of(
                        System.getProperty("java.home"),
                        "bin",
                        System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java")
                .toString();
        return List.of(
                javaCommand,
                "-cp",
                System.getProperty("java.class.path"),
                probe.getName());
    }

    @Test
    void killsDescendantProcessesWhenTheCommandTimesOut() throws Exception {
        ProcessBuilderCliCommandRunner runner = new ProcessBuilderCliCommandRunner(scheduler);
        Path childPidFile = tempDirectory.resolve("child.pid");
        List<String> command = new java.util.ArrayList<>(javaCommand(ParentSpawnsChildProbe.class));
        command.add(childPidFile.toString());
        CliCommandRequest request = new CliCommandRequest(
                "SpotiFLAC",
                "track-timeout",
                "job-timeout",
                command,
                Duration.ofSeconds(1),
                true);

        StepVerifier.create(runner.run(request))
                .expectError(TimeoutException.class)
                .verify(Duration.ofSeconds(10));

        long childPid = awaitChildPid(childPidFile, Duration.ofSeconds(5));
        ProcessHandle child = ProcessHandle.of(childPid).orElse(null);
        try {
            assertThat(child == null || !child.isAlive()).isTrue();
        } finally {
            if (child != null && child.isAlive()) {
                child.destroyForcibly();
                child.onExit().get(5, TimeUnit.SECONDS);
            }
        }
    }

    private long awaitChildPid(Path pidFile, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.exists(pidFile)) {
                return Long.parseLong(Files.readString(pidFile).trim());
            }
            Thread.sleep(20L);
        }
        throw new IllegalStateException("Child PID file was not created");
    }

    public static final class ParentSpawnsChildProbe {
        public static void main(String[] args) throws Exception {
            String javaCommand = Path.of(
                            System.getProperty("java.home"),
                            "bin",
                            System.getProperty("os.name").startsWith("Windows")
                                    ? "java.exe"
                                    : "java")
                    .toString();
            Process child = new ProcessBuilder(
                    javaCommand,
                    "-cp",
                    System.getProperty("java.class.path"),
                    ChildSleeperProbe.class.getName())
                    .start();
            Files.writeString(Path.of(args[0]), Long.toString(child.pid()));
            System.out.println("CHILD_PID=" + child.pid());
            System.out.flush();
            Thread.sleep(60_000L);
        }
    }

    public static final class ChildSleeperProbe {
        public static void main(String[] args) throws Exception {
            Thread.sleep(60_000L);
        }
    }

    public static final class StreamingOutputProbe {
        public static void main(String[] args) throws Exception {
            System.out.println("FIRST_OUTPUT");
            System.out.println("Access token acquired: very-secret-token");
            System.out.println("set-cookie: private-cookie");
            System.out.flush();
            Thread.sleep(3_000L);
            System.out.println("SECOND_OUTPUT");
        }
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
