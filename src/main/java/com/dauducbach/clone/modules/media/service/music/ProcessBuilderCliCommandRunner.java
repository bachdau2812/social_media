package com.dauducbach.clone.modules.media.service.music;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

@Component
public class ProcessBuilderCliCommandRunner implements CliCommandRunner {
    private static final Logger log = LoggerFactory.getLogger(ProcessBuilderCliCommandRunner.class);
    private static final int MAX_CAPTURED_OUTPUT_BYTES = 64 * 1024;
    private static final Pattern ACCESS_TOKEN_LINE = Pattern.compile(
            "(?i)(access\\s+token\\s+acquired\\s*:\\s*)\\S+");
    private static final Pattern SENSITIVE_HEADER_LINE = Pattern.compile(
            "(?i)(set-cookie|cookie|authorization)\\s*:\\s*.*$");
    private static final Pattern SENSITIVE_HEADER_TUPLE = Pattern.compile(
            "(?i)\\(b?'(set-cookie|cookie|authorization)',\\s*b?'[^']*'\\)");

    private final Scheduler scheduler;

    public ProcessBuilderCliCommandRunner(
            @Qualifier("spotifyMusicProcessScheduler") Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Mono<CliCommandResult> run(CliCommandRequest request) {
        if (request == null
                || request.command() == null
                || request.command().isEmpty()
                || request.command().stream().anyMatch(this::isBlank)) {
            return Mono.error(new IllegalArgumentException("CLI command and arguments are required"));
        }
        if (request.timeout() == null
                || request.timeout().isZero()
                || request.timeout().isNegative()) {
            return Mono.error(new IllegalArgumentException("CLI timeout must be greater than zero"));
        }
        if (isBlank(request.label()) || isBlank(request.trackId()) || isBlank(request.jobId())) {
            return Mono.error(new IllegalArgumentException("CLI logging context is required"));
        }

        return Mono.fromCallable(() -> execute(request))
                .subscribeOn(scheduler);
    }

    private CliCommandResult execute(CliCommandRequest request) throws Exception {
        List<String> command = request.command();
        Duration timeout = request.timeout();
        long startedAtNanos = System.nanoTime();
        if (request.logOutput()) {
            log.info(
                    "[music-fetch] trackId={} jobId={} command={}",
                    request.trackId(),
                    request.jobId(),
                    command);
        }
        ProcessBuilder processBuilder = new ProcessBuilder(command)
                .redirectErrorStream(true);
        processBuilder.environment().put("PYTHONUTF8", "1");
        processBuilder.environment().put("PYTHONIOENCODING", "utf-8");
        Process process = processBuilder.start();
        long pid = process.pid();
        String executable = process.info().command().orElse(command.getFirst());
        if (request.logOutput()) {
            log.info(
                    "[music-fetch] trackId={} jobId={} {} pid={} executable={}",
                    request.trackId(),
                    request.jobId(),
                    request.label(),
                    pid,
                    executable);
        }
        process.getOutputStream().close();
        ExecutorService outputExecutor = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("spotify-cli-output-", 0).factory());
        Future<String> output = outputExecutor.submit(
                () -> readCapped(process.getInputStream(), request));
        try {
            boolean completed = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!completed) {
                log.error(
                        "[music-fetch] trackId={} jobId={} process timeout pid={} elapsedMs={}",
                        request.trackId(),
                        request.jobId(),
                        pid,
                        elapsedMillis(startedAtNanos));
                terminateProcessTree(process, request);
                throw new TimeoutException("CLI command timed out after " + timeout);
            }
            int exitCode = process.exitValue();
            String capturedOutput = output.get(5, TimeUnit.SECONDS);
            if (request.logOutput()) {
                log.info(
                        "[music-fetch] trackId={} jobId={} process finished exitCode={} elapsedMs={}",
                        request.trackId(),
                        request.jobId(),
                        exitCode,
                        elapsedMillis(startedAtNanos));
            }
            return new CliCommandResult(exitCode, capturedOutput);
        } finally {
            if (process.isAlive()) {
                terminateProcessTree(process, request);
            }
            outputExecutor.shutdownNow();
        }
    }

    private String readCapped(InputStream inputStream, CliCommandRequest request) throws Exception {
        try (inputStream;
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(inputStream, StandardCharsets.UTF_8));
             ByteArrayOutputStream captured = new ByteArrayOutputStream()) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (request.logOutput()) {
                    log.info("[{}] {}", request.label(), sanitizeOutputLine(line));
                }
                appendCapped(captured, line);
                appendCapped(captured, System.lineSeparator());
            }
            return captured.toString(StandardCharsets.UTF_8);
        }
    }

    private String sanitizeOutputLine(String line) {
        String sanitized = ACCESS_TOKEN_LINE.matcher(line).replaceAll("$1[REDACTED]");
        sanitized = SENSITIVE_HEADER_TUPLE.matcher(sanitized).replaceAll("$1=[REDACTED]");
        return SENSITIVE_HEADER_LINE.matcher(sanitized).replaceAll("$1=[REDACTED]");
    }

    private void appendCapped(ByteArrayOutputStream captured, String value) {
        int remaining = MAX_CAPTURED_OUTPUT_BYTES - captured.size();
        if (remaining <= 0) {
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        captured.write(bytes, 0, Math.min(bytes.length, remaining));
    }

    private long elapsedMillis(long startedAtNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos);
    }

    private void terminateProcessTree(Process process, CliCommandRequest request) {
        ProcessHandle root = process.toHandle();
        Map<Long, ProcessHandle> descendantsByPid = new LinkedHashMap<>();
        snapshotDescendants(root).forEach(handle -> descendantsByPid.put(handle.pid(), handle));
        descendantsByPid.values().forEach(handle -> destroyDescendant(handle, request, false));
        snapshotDescendants(root).forEach(handle -> {
            if (!descendantsByPid.containsKey(handle.pid())) {
                descendantsByPid.put(handle.pid(), handle);
                destroyDescendant(handle, request, false);
            }
        });
        destroyHandle(root, false);
        awaitExit(descendantsByPid.values(), root, Duration.ofSeconds(2));

        descendantsByPid.values().stream()
                .filter(ProcessHandle::isAlive)
                .forEach(handle -> destroyDescendant(handle, request, true));
        if (root.isAlive()) {
            log.warn(
                    "[music-fetch] trackId={} jobId={} forcibly killing parent pid={}",
                    request.trackId(),
                    request.jobId(),
                    root.pid());
            destroyHandle(root, true);
        }
        awaitExit(descendantsByPid.values(), root, Duration.ofSeconds(2));
    }

    private List<ProcessHandle> snapshotDescendants(ProcessHandle root) {
        return root.descendants()
                .sorted(Comparator.comparingInt(
                                (ProcessHandle handle) -> processDepth(handle, root))
                        .reversed())
                .toList();
    }

    private int processDepth(ProcessHandle handle, ProcessHandle root) {
        int depth = 0;
        ProcessHandle current = handle;
        while (current.pid() != root.pid() && current.parent().isPresent()) {
            depth++;
            current = current.parent().orElse(root);
        }
        return depth;
    }

    private void destroyDescendant(
            ProcessHandle handle,
            CliCommandRequest request,
            boolean forcibly) {
        if (!handle.isAlive()) {
            return;
        }
        log.warn(
                "[music-fetch] trackId={} jobId={} killing descendant pid={} forcibly={}",
                request.trackId(),
                request.jobId(),
                handle.pid(),
                forcibly);
        destroyHandle(handle, forcibly);
    }

    private void destroyHandle(ProcessHandle handle, boolean forcibly) {
        try {
            if (forcibly) {
                handle.destroyForcibly();
            } else {
                handle.destroy();
            }
        } catch (RuntimeException error) {
            log.warn(
                    "[music-fetch] failed to terminate pid={} forcibly={} error={}",
                    handle.pid(),
                    forcibly,
                    error.getMessage());
        }
    }

    private void awaitExit(
            Iterable<ProcessHandle> descendants,
            ProcessHandle root,
            Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            while (System.nanoTime() < deadline) {
                boolean descendantAlive = false;
                for (ProcessHandle descendant : descendants) {
                    if (descendant.isAlive()) {
                        descendantAlive = true;
                        break;
                    }
                }
                if (!root.isAlive() && !descendantAlive) {
                    return;
                }
                Thread.sleep(25L);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
