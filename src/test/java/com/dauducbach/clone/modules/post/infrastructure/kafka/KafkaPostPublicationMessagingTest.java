package com.dauducbach.clone.modules.post.infrastructure.kafka;

import com.dauducbach.clone.modules.post.dto.event.PostMediaScanItem;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.service.post.PostSseService;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.kafka.annotation.KafkaListener;
import com.dauducbach.clone.modules.post.service.post.ImageScanWorker;
import org.reactivestreams.Publisher;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaPostPublicationMessagingTest {
    @SuppressWarnings("unchecked")
    private final KafkaSender<String, String> kafkaSender = mock(KafkaSender.class);
    private final PostSseService postSseService = mock(PostSseService.class);
    private final KafkaPostPublicationMessaging messaging = new KafkaPostPublicationMessaging(kafkaSender, postSseService);

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void preservesExistingTopicsKeysPayloadAndSseName() {
        doReturn(Flux.empty()).when(kafkaSender).<String>send(any());
        when(postSseService.sendToUser(eq("user-1"), eq("post_upload"), any())).thenReturn(Mono.empty());
        PostDetails post = PostDetails.builder()
                .postId("post-1").userId("user-1").content("hello").mediaRatio("4:3")
                .musicId("music-1").musicStart(2L).musicEnd(9L).build();

        messaging.requestMediaScan("post-1", "user-1", List.of(PostMediaScanItem.builder()
                .orderNumber(1).secureUrl("https://media.test/image.jpg").publicId("image-1").resourceType("image").build())).block();
        messaging.publishApproved(post, "done").block();
        messaging.publishUpdated(post).block();

        ArgumentCaptor<Publisher<SenderRecord<String, String, String>>> captor =
                (ArgumentCaptor) org.mockito.ArgumentCaptor.forClass(Publisher.class);
        verify(kafkaSender, times(3)).<String>send(captor.capture());
        List<SenderRecord<String, String, String>> records = captor.getAllValues().stream()
                .map(publisher -> Flux.from(publisher).blockFirst())
                .toList();

        assertThat(records).extracting(SenderRecord::topic)
                .containsExactly("check_media_event", "post_upload_event", "post_update_event");
        assertThat(records).extracting(SenderRecord::key)
                .containsExactly("post-1", "post-1", "post-1");
        assertThat(records.get(0).value()).contains("\"postId\":\"post-1\"").contains("\"items\"");
        assertThat(records.get(1).value()).contains("\"userId\":\"user-1\"")
                .contains("\"musicId\":\"music-1\"").contains("\"mediaRatio\":\"4:3\"");
        assertThat(records.get(2).value()).contains("\"post_id\":\"post-1\"").contains("\"content\":\"hello\"");
        verify(postSseService).sendToUser(eq("user-1"), eq("post_upload"), org.mockito.ArgumentMatchers.argThat(value ->
                value.contains("\"postId\":\"post-1\"") && value.contains("\"result\":\"SUCCESSED\"")
                        && value.contains("\"message\":\"done\"")));
    }

    @Test
    @SuppressWarnings("unchecked")
    void propagatesKafkaSenderResultFailuresForUpdatedEvents() {
        RuntimeException failure = new IllegalStateException("broker rejected");
        SenderResult<String> result = mock(SenderResult.class);
        when(result.exception()).thenReturn(failure);
        doReturn(Flux.just(result)).when(kafkaSender).<String>send(any());
        PostDetails post = PostDetails.builder().postId("post-1").userId("user-1").content("hello").build();

        StepVerifier.create(messaging.publishUpdated(post)).expectErrorMatches(error -> error == failure).verify();
    }

    @Test
    @SuppressWarnings("unchecked")
    void propagatesKafkaSenderResultFailuresForMediaScanRequests() {
        RuntimeException failure = new IllegalStateException("broker rejected");
        SenderResult<String> result = mock(SenderResult.class);
        when(result.exception()).thenReturn(failure);
        doReturn(Flux.just(result)).when(kafkaSender).<String>send(any());

        StepVerifier.create(messaging.requestMediaScan("post-1", "user-1", List.of()))
                .expectErrorMatches(error -> error == failure)
                .verify();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void configuredScanTopicIsSharedByPublisherAndWorkerWithoutChangingPublicationTopics() throws Exception {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test", Map.of(
                    "post.media.scan.topic", "check_media_event_test_local",
                    "post.media.scan.consumer-group", "post-media-scan-test-local")));
            context.registerBean(KafkaSender.class, () -> kafkaSender);
            context.registerBean(PostSseService.class, () -> postSseService);
            context.registerBean(KafkaPostPublicationMessaging.class);
            context.refresh();
            doReturn(Flux.empty()).when(kafkaSender).<String>send(any());
            var configured = context.getBean(KafkaPostPublicationMessaging.class);
            configured.requestMediaScan("post-1", "user-1", List.of()).block();
            configured.publishUpdated(PostDetails.builder().postId("post-1").build()).block();
            ArgumentCaptor<Publisher<SenderRecord<String, String, String>>> captor =
                    (ArgumentCaptor) ArgumentCaptor.forClass(Publisher.class);
            verify(kafkaSender, times(2)).<String>send(captor.capture());
            assertThat(captor.getAllValues().stream().map(p -> Flux.from(p).blockFirst().topic()).toList())
                    .containsExactly("check_media_event_test_local", "post_update_event");
            KafkaListener listener = ImageScanWorker.class.getMethod("handlePostScanEvent", String.class)
                    .getAnnotation(KafkaListener.class);
            assertThat(context.getEnvironment().resolveRequiredPlaceholders(listener.topics()[0]))
                    .isEqualTo("check_media_event_test_local");
            assertThat(context.getEnvironment().resolveRequiredPlaceholders(listener.groupId()))
                    .isEqualTo("post-media-scan-test-local");
        }
    }
}
