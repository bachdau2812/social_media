package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.infrastructure.outbox.InteractionOutbox;
import com.dauducbach.clone.modules.post.dto.request.PostInteractionRequest;
import com.dauducbach.clone.modules.post.dto.response.PostInteractionAcceptedResponse;
import com.dauducbach.clone.modules.post.dto.event.PostInteractionEvent;
import com.dauducbach.clone.modules.post.entity.PostInteractionReceipt;
import com.dauducbach.clone.modules.post.repository.PostInteractionReceiptRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.time.Duration;
import java.util.UUID;
import java.util.HexFormat;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
@RequiredArgsConstructor
public class PostInteractionService {
    private static final Logger log = LoggerFactory.getLogger(PostInteractionService.class);
    private final PostInteractionReceiptRepository receipts;
    private final PostFeedQueryService posts;
    private final InteractionOutbox outbox;
    private final PostInteractionScorePolicy scores;
    @Value("${post.popularity.ingestion-enabled:false}")
    private boolean ingestionEnabled;

    public Mono<PostInteractionAcceptedResponse> accept(String actor, PostInteractionRequest input) {
        return Mono.defer(() -> {
            if (!ingestionEnabled) return Mono.error(new AppException(ErrorCode.POST_INTERACTION_UNAVAILABLE));
            if (actor == null || actor.isBlank() || input == null || input.postId() == null || input.postId().isBlank()
                    || input.isClick() == null || input.viewTime() == null || input.viewTime() < 0 || input.viewTime() > 3600)
                return Mono.error(new AppException(ErrorCode.POST_INTERACTION_INVALID));
            String event = canonicalUuid(input.eventId());
            String impression = canonicalUuid(input.impressionId());
            String post = input.postId().trim();
            String hash = hash(post, impression, input.isClick(), input.viewTime());
            return receipts.find(actor, event).flatMap(existing -> response(existing, hash, true))
                    .switchIfEmpty(Mono.defer(() -> posts.getApprovedPostById(post)
                            .switchIfEmpty(Mono.error(new AppException(ErrorCode.POST_NOT_FOUND)))
                            .flatMap(approved -> {
                                PostInteractionReceipt receipt = new PostInteractionReceipt(actor, event, post, impression,
                                        hash, scores.score(input.isClick(), input.viewTime()), Instant.now());
                                return outbox.commit(receipts.insert(receipt), saved -> {
                                    var payload = new PostInteractionEvent(event, post, actor, impression, input.isClick(),
                                            input.viewTime(), saved.computedScore(), saved.acceptedAt()).toJson();
                                    return outbox.append("POST_INTERACTION:" + actor + ":" + event,
                                            "post_interaction", actor, post, payload, saved.acceptedAt());
                                }).flatMap(saved -> response(saved, hash, false));
                            })
                            // Recovery subscribes after the transactional publisher has rolled back.
                            .onErrorResume(DuplicateKeyException.class, error -> receipts.find(actor, event)
                                    .switchIfEmpty(Mono.error(error)).flatMap(existing -> response(existing, hash, true)))));
        });
    }

    private Mono<PostInteractionAcceptedResponse> response(PostInteractionReceipt receipt, String hash, boolean duplicate) {
        return receipt.payloadHash().equals(hash)
                ? Mono.just(new PostInteractionAcceptedResponse(receipt.eventId(), receipt.computedScore(), duplicate))
                : Mono.error(new AppException(ErrorCode.POST_INTERACTION_BODY_CONFLICT));
    }
    private String canonicalUuid(String raw) {
        try {
            if (raw == null || !raw.matches("(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))
                throw new IllegalArgumentException();
            return UUID.fromString(raw).toString();
        } catch (IllegalArgumentException error) { throw new AppException(ErrorCode.POST_INTERACTION_INVALID); }
    }
    private String hash(String post, String impression, boolean click, int seconds) {
        try {
            // Length-prefixed post ID makes field boundaries unambiguous.
            String body = post.length() + ":" + post + ":" + impression + ":" + click + ":" + seconds;
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException error) { throw new IllegalStateException(error); }
    }
    @Scheduled(fixedDelayString = "${post.popularity.receipt-cleanup-delay-ms:3600000}")
    public Mono<Void> cleanupReceipts() {
        if (!ingestionEnabled) return Mono.empty();
        return Mono.defer(() -> receipts.deleteExpired(Instant.now().minus(Duration.ofDays(8)), 1000))
                .then()
                .doOnError(error -> log.warn("Receipt cleanup failed", error));
    }
}
