package com.dauducbach.clone.modules.post.listener;

import com.dauducbach.clone.configuration.PostPopularityProjectionConfig;
import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.post.dto.event.PostEventJson;
import com.dauducbach.clone.modules.post.dto.event.PostPopularityUpdate;
import com.dauducbach.clone.modules.post.service.post.PostPopularityProjectionService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/** Real annotated listener adapters and forwarded Kafka; every topic/group belongs to this test. */
@EnabledIfEnvironmentVariable(named = "POPULARITY_TEST_KAFKA_BOOTSTRAP", matches = ".+")
class PostPopularityProjectionKafkaIntegrationTest {
    @Test void redisFailureIsRetriedBeforeSourceOffsetAdvances() throws Exception {
        try (Fixture fixture = new Fixture()) {
            AtomicInteger attempts = new AtomicInteger();
            var redis = mockRedis();
            var projection = new PostPopularityProjectionService(redis, new PostPopularityProperties());
            when(redis.execute(any(RedisScript.class), anyList(), anyList())).thenAnswer(call ->
                    Flux.defer(() -> attempts.incrementAndGet() == 1
                            ? Flux.error(new IllegalStateException("Redis transient failure")) : Flux.just(1L)));
            var container = fixture.container(projection);
            try {
                container.start();
                fixture.publish();
                await(() -> attempts.get() >= 2, Duration.ofSeconds(25), "Redis failure must trigger a second delivery");
                await(() -> fixture.committedOffset() >= 1, Duration.ofSeconds(15), "Successful retry must commit");
                assertTrue(fixture.dlt.poll(Duration.ofMillis(500)).isEmpty());
            } finally {
                container.stop();
            }
        }
    }

    @Test void durableDeadLetterMustSucceedBeforeFailedProjectionOffsetAdvances() throws Exception {
        try (Fixture fixture = new Fixture()) {
            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger failedDeadLetters = new AtomicInteger();
            AtomicBoolean failDeadLetter = new AtomicBoolean(true);
            var redis = mockRedis();
            var projection = new PostPopularityProjectionService(redis, new PostPopularityProperties());
            when(redis.execute(any(RedisScript.class), anyList(), anyList())).thenAnswer(call -> Flux.defer(() -> {
                attempts.incrementAndGet();
                return Flux.error(new IllegalStateException("Redis persistent failure"));
            }));
            var template = new KafkaTemplate<String, String>(fixture.producers) {
                @Override public CompletableFuture<SendResult<String, String>> send(ProducerRecord<String, String> record) {
                    if (record.topic().equals(fixture.topic + ".DLT") && failDeadLetter.get()) {
                        failedDeadLetters.incrementAndGet();
                        return CompletableFuture.failedFuture(new IllegalStateException("DLT unavailable"));
                    }
                    return super.send(record);
                }
            };
            var productionFactory = new PostPopularityProjectionConfig().postPopularityListenerFactory(
                    fixture.context.getBean(KafkaProperties.class), template);
            var container = fixture.container(projection, productionFactory);
            try {
                container.start();
                fixture.publish();
                await(() -> failedDeadLetters.get() >= 2, Duration.ofSeconds(25), "Failed DLT send must be retried");
                assertTrue(attempts.get() >= 2, "Persistent Redis errors must be delivered again");
                assertTrue(fixture.committedOffset() < 1, "DLT failure must prevent source offset success");
                assertTrue(fixture.dlt.poll(Duration.ofMillis(200)).isEmpty());
                failDeadLetter.set(false);
                String deadLetter = null;
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (deadLetter == null && System.nanoTime() < deadline) {
                    for (var record : fixture.dlt.poll(Duration.ofMillis(200))) deadLetter = record.value();
                }
                assertEquals(fixture.payload, deadLetter, "Original failed payload must be durably published");
                await(() -> fixture.committedOffset() >= 1, Duration.ofSeconds(15), "Recovered record must commit after DLT");
            } finally {
                container.stop();
                template.destroy();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static ReactiveRedisTemplate<String, String> mockRedis() {
        return mock(ReactiveRedisTemplate.class);
    }

    private static void await(BooleanSupplier condition, Duration timeout, String message) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(50);
        assertTrue(condition.getAsBoolean(), message);
    }

    private static class Fixture implements AutoCloseable {
        final String topic = "test-popularity-projection-" + UUID.randomUUID();
        final String group = topic + "-group";
        final AdminClient admin;
        final AnnotationConfigApplicationContext context;
        final ConcurrentKafkaListenerContainerFactory<String, String> factory;
        final DefaultKafkaProducerFactory<String, String> producers;
        final KafkaTemplate<String, String> producer;
        final KafkaConsumer<String, String> dlt;
        final String payload;

        @SuppressWarnings("unchecked")
        Fixture() throws Exception {
            String bootstrap = System.getenv("POPULARITY_TEST_KAFKA_BOOTSTRAP");
            admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap));
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1), new NewTopic(topic + ".DLT", 1, (short) 1)))
                    .all().get(15, TimeUnit.SECONDS);
            KafkaProperties kafka = new KafkaProperties();
            kafka.setBootstrapServers(List.of(bootstrap));
            context = new AnnotationConfigApplicationContext();
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test",
                    Map.of("post.popularity.projection-enabled", "true")));
            context.registerBean(KafkaProperties.class, () -> kafka);
            context.register(PostPopularityProjectionConfig.class);
            context.refresh();
            factory = (ConcurrentKafkaListenerContainerFactory<String, String>) context.getBean("postPopularityListenerFactory");
            Map<String, Object> config = new HashMap<>();
            config.put("bootstrap.servers", bootstrap);
            config.put("key.serializer", StringSerializer.class);
            config.put("value.serializer", StringSerializer.class);
            config.put("acks", "all");
            producers = new DefaultKafkaProducerFactory<>(config);
            producer = new KafkaTemplate<>(producers);
            Map<String, Object> consumer = new HashMap<>();
            consumer.put("bootstrap.servers", bootstrap);
            consumer.put(ConsumerConfig.GROUP_ID_CONFIG, topic + "-dlt-verify");
            consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            dlt = new KafkaConsumer<>(consumer);
            dlt.subscribe(List.of(topic + ".DLT"));
            Instant now = Instant.now();
            payload = PostEventJson.json(new PostPopularityUpdate(1, "POPULAR_V1:post:" + now.toEpochMilli(), "post", now, now.plusSeconds(172800), 101, "popular-v1"));
        }

        org.springframework.kafka.listener.ConcurrentMessageListenerContainer<String, String> container(
                PostPopularityProjectionService projection) throws Exception {
            return container(projection, factory);
        }

        org.springframework.kafka.listener.ConcurrentMessageListenerContainer<String, String> container(
                PostPopularityProjectionService projection,
                ConcurrentKafkaListenerContainerFactory<String, String> listenerFactory) throws Exception {
            var endpoint = new MethodKafkaListenerEndpoint<String, String>();
            endpoint.setId(group);
            endpoint.setGroupId(group);
            endpoint.setTopics(topic);
            endpoint.setBean(new PostPopularityProjectionListener(projection));
            endpoint.setMethod(PostPopularityProjectionListener.class.getMethod("receive", org.apache.kafka.clients.consumer.ConsumerRecord.class));
            var handlers = new DefaultMessageHandlerMethodFactory();
            handlers.afterPropertiesSet();
            endpoint.setMessageHandlerMethodFactory(handlers);
            return listenerFactory.createListenerContainer(endpoint);
        }

        void publish() throws Exception {
            producer.send(new ProducerRecord<>(topic, "post", payload)).get(10, TimeUnit.SECONDS);
        }

        long committedOffset() {
            try {
                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                var offset = offsets.get(new TopicPartition(topic, 0));
                return offset == null ? -1 : offset.offset();
            } catch (Exception error) {
                throw new IllegalStateException("Cannot inspect test consumer offset", error);
            }
        }

        @Override public void close() throws Exception {
            dlt.close(Duration.ofSeconds(5));
            producer.destroy();
            producers.destroy();
            context.close();
            admin.deleteTopics(List.of(topic, topic + ".DLT")).all().get(15, TimeUnit.SECONDS);
            admin.close(Duration.ofSeconds(5));
        }
    }
}
