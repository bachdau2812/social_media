package com.dauducbach.clone.modules.media.service.music;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class ProcessBuilderCliCommandRunner implements CliCommandRunner {
    private static final int MAX_CAPTURED_OUTPUT_BYTES = 64 * 1024;

    private final Scheduler scheduler;

    public ProcessBuilderCliCommandRunner(
            @Qualifier("spotifyMusicProcessScheduler") Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Mono<CliCommandResult> run(List<String> command, Duration timeout) {
        if (command == null || command.isEmpty() || command.stream().anyMatch(this::isBlank)) {
            return Mono.error(new IllegalArgumentException("CLI command and arguments are required"));
        }
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return Mono.error(new IllegalArgumentException("CLI timeout must be greater than zero"));
        }

        List<String> immutableCommand = List.copyOf(command);
        return Mono.fromCallable(() -> execute(immutableCommand, timeout))
                .subscribeOn(scheduler);
    }

    private CliCommandResult execute(List<String> command, Duration timeout) throws Exception {
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        ExecutorService outputExecutor = Executors.newSingleThreadExecutor(
                Thread.ofVirtual().name("spotify-cli-output-", 0).factory());
        Future<String> output = outputExecutor.submit(() -> readCapped(process.getInputStream()));
        try {
            boolean completed = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!completed) {
                process.destroy();
                if (!process.waitFor(2, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(2, TimeUnit.SECONDS);
                }
                throw new TimeoutException("CLI command timed out after " + timeout);
            }
            return new CliCommandResult(process.exitValue(), output.get(5, TimeUnit.SECONDS));
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
            }
            outputExecutor.shutdownNow();
        }
    }

    private String readCapped(InputStream inputStream) throws Exception {
        try (inputStream; ByteArrayOutputStream captured = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                int remaining = MAX_CAPTURED_OUTPUT_BYTES - captured.size();
                if (remaining > 0) {
                    captured.write(buffer, 0, Math.min(read, remaining));
                }
            }
            return captured.toString(StandardCharsets.UTF_8);
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
