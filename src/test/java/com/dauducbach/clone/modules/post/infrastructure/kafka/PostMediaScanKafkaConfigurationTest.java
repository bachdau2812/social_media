package com.dauducbach.clone.modules.post.infrastructure.kafka;

import com.dauducbach.clone.modules.post.service.post.ImageScanWorker;
import com.dauducbach.clone.modules.post.service.post.PostSseService;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.reactivestreams.Publisher;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mock.env.MockEnvironment;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PostMediaScanKafkaConfigurationTest {
    @Test
    void defaultProfileKeepsExistingProductionTopicAndGroup() throws Exception {
        try (var context = new AnnotationConfigApplicationContext()) {
            context.setEnvironment(new MockEnvironment());
            context.register(PostMediaScanKafkaConfiguration.class);
            context.refresh();
            KafkaListener listener = listener();
            assertThat(context.getBean(NewTopic.class).name()).isEqualTo("check_media_event");
            assertThat(context.getEnvironment().resolveRequiredPlaceholders(listener.topics()[0])).isEqualTo("check_media_event");
            assertThat(context.getEnvironment().resolveRequiredPlaceholders(listener.groupId())).isEqualTo("post-service");
        }
    }

    @Test
    void servProfileKeepsExistingScanTopicAndGroup() throws Exception {
        assertServRouting(new MockEnvironment(), "check_media_event", "post-service");
    }

    @Test
    void environmentOverridesKeepTopicCreationPublisherAndListenerAligned() throws Exception {
        MockEnvironment environment = new MockEnvironment()
                .withProperty("POST_MEDIA_SCAN_TOPIC", "custom_media_scan_event")
                .withProperty("POST_MEDIA_SCAN_CONSUMER_GROUP", "custom-media-scan-group");
        assertServRouting(environment, "custom_media_scan_event", "custom-media-scan-group");
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void assertServRouting(MockEnvironment environment, String topic, String group) throws Exception {
        new YamlPropertySourceLoader().load("serv", new ClassPathResource("application-serv.yaml"))
                .forEach(environment.getPropertySources()::addLast);
        try (var context = new AnnotationConfigApplicationContext()) {
            context.setEnvironment(environment);
            KafkaSender<String, String> sender = mock(KafkaSender.class);
            doReturn(Flux.empty()).when(sender).<String>send(any());
            context.registerBean(KafkaSender.class, () -> sender);
            context.registerBean(PostSseService.class, () -> mock(PostSseService.class));
            context.register(PostMediaScanKafkaConfiguration.class, KafkaPostPublicationMessaging.class);
            context.refresh();
            context.getBean(KafkaPostPublicationMessaging.class).requestMediaScan("post-1", "user-1", List.of()).block();
            ArgumentCaptor<Publisher<SenderRecord<String, String, String>>> captor =
                    (ArgumentCaptor) ArgumentCaptor.forClass(Publisher.class);
            verify(sender).<String>send(captor.capture());
            assertThat(Flux.from(captor.getValue()).blockFirst().topic()).isEqualTo(topic);
            assertThat(context.getBean(NewTopic.class).name()).isEqualTo(topic);
            assertThat(environment.resolveRequiredPlaceholders(listener().topics()[0])).isEqualTo(topic);
            assertThat(environment.resolveRequiredPlaceholders(listener().groupId())).isEqualTo(group);
        }
    }

    private KafkaListener listener() throws Exception {
        return ImageScanWorker.class.getMethod("handlePostScanEvent", String.class).getAnnotation(KafkaListener.class);
    }
}
