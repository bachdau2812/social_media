package com.dauducbach.clone.modules.post.infrastructure.kafka;

import com.dauducbach.clone.modules.post.application.PostPublicationMessaging;
import com.dauducbach.clone.modules.post.constant.PostMediaRatio;
import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.service.post.PostSseService;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.List;

@Component
@RequiredArgsConstructor
public class KafkaPostPublicationMessaging implements PostPublicationMessaging {
    private static final Logger log = LoggerFactory.getLogger(KafkaPostPublicationMessaging.class);

    private final KafkaSender<String, String> kafkaSender;
    private final PostSseService postSseService;

    @Override
    public Mono<Void> requestMediaScan(String postId, String userId, List<PostMediaScanItem> items) {
        JsonObject payload = new JsonObject();
        payload.addProperty("postId", postId);
        payload.addProperty("userId", userId);
        payload.add("items", GsonUtils.getGson().toJsonTree(items));
        return kafkaSender.send(Mono.just(record("check_media_event", postId, payload.toString())))
                .then()
                .doOnError(error -> log.error("|KafkaPostPublicationMessaging|requestMediaScan|postId={}|error={}", postId, error.getMessage()))
                .doOnSuccess(unused -> log.info("|KafkaPostPublicationMessaging|requestMediaScan|sent|postId={}|userId={}|itemCount={}",
                        postId, userId, items.size()));
    }

    @Override
    public Mono<Void> publishApproved(PostDetails post, String successMessage) {
        JsonObject payload = new JsonObject();
        payload.addProperty("postId", post.getPostId());
        payload.addProperty("result", "SUCCESSED");
        payload.addProperty("message", successMessage);
        return postSseService.sendToUser(post.getUserId(), "post_upload", payload.toString())
                .doOnSuccess(unused -> log.info("|KafkaPostPublicationMessaging|publishApproved|sseSent|postId={}|userId={}",
                        post.getPostId(), post.getUserId()))
                .then(send("post_upload_event", post.getPostId(), buildPostEventPayload(post).toString())
                        .doOnError(error -> log.error("|KafkaPostPublicationMessaging|publishApproved|postId={}|error={}", post.getPostId(), error.getMessage()))
                        .doOnSuccess(unused -> log.info("|KafkaPostPublicationMessaging|publishApproved|eventSent|postId={}|userId={}",
                                post.getPostId(), post.getUserId())));
    }

    @Override
    public Mono<Void> publishUpdated(PostDetails post) {
        JsonObject payload = new JsonObject();
        payload.addProperty("post_id", post.getPostId());
        payload.addProperty("content", post.getContent());
        payload.addProperty("mediaRatio", PostMediaRatio.defaultIfMissing(post.getMediaRatio()));
        payload.add("hashtag", GsonUtils.getGson().toJsonTree(post.getHashtagList()));
        return send("post_update_event", post.getPostId(), payload.toString())
                .doOnError(error -> log.error("|KafkaPostPublicationMessaging|publishUpdated|postId={}|error={}", post.getPostId(), error.getMessage()))
                .doOnSuccess(unused -> log.info("|KafkaPostPublicationMessaging|publishUpdated|sent|postId={}", post.getPostId()));
    }

    private Mono<Void> send(String topic, String key, String payload) {
        return kafkaSender.send(Mono.just(record(topic, key, payload)))
                .flatMap(result -> result.exception() == null ? Mono.just(result) : Mono.error(result.exception()))
                .then();
    }

    private SenderRecord<String, String, String> record(String topic, String key, String payload) {
        return SenderRecord.create(new ProducerRecord<>(topic, key, payload), topic);
    }

    private JsonObject buildPostEventPayload(PostDetails post) {
        JsonObject payload = new JsonObject();
        payload.addProperty("post_id", post.getPostId());
        payload.addProperty("userId", post.getUserId());
        payload.addProperty("content", post.getContent());
        payload.addProperty("mediaRatio", PostMediaRatio.defaultIfMissing(post.getMediaRatio()));
        payload.add("hashtag", GsonUtils.getGson().toJsonTree(post.getHashtagList()));
        if (post.getMusicId() != null && !post.getMusicId().isBlank()) {
            payload.addProperty("musicId", post.getMusicId());
            payload.addProperty("musicStart", post.getMusicStart());
            payload.addProperty("musicEnd", post.getMusicEnd());
        }
        return payload;
    }
}
