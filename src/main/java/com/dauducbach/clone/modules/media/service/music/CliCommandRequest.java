package com.dauducbach.clone.modules.media.service.music;

import java.time.Duration;
import java.util.List;

public record CliCommandRequest(
        String label,
        String trackId,
        String jobId,
        List<String> command,
        Duration timeout,
        boolean logOutput) {

    public CliCommandRequest {
        command = command == null ? null : List.copyOf(command);
    }

    public static CliCommandRequest quiet(List<String> command, Duration timeout) {
        return new CliCommandRequest("CLI", "-", "-", command, timeout, false);
    }
}