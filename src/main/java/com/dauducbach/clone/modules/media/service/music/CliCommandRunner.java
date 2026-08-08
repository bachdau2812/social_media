package com.dauducbach.clone.modules.media.service.music;

import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

public interface CliCommandRunner {
    Mono<CliCommandResult> run(List<String> command, Duration timeout);
}
