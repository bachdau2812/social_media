package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.infrastructure.realtime.AccountSseHub;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.security.Principal;

@RestController
@RequiredArgsConstructor
@RequestMapping("/posts/sse")
public class PostSseController {
    private final AccountSseHub accountSseHub;

    @GetMapping(value = "/{userId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> streamForUser(@PathVariable String userId, Principal principal) {
        if (principal == null || !userId.equals(principal.getName())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Cannot subscribe to another account's stream");
        }
        return accountSseHub.subscribe(userId);
    }


}
