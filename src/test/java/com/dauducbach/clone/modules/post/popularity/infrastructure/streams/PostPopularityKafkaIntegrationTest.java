package com.dauducbach.clone.modules.post.popularity.infrastructure.streams;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.post.dto.event.PostInteractionEvent;
import com.dauducbach.clone.modules.post.dto.event.PostPopularityUpdate;
import com.dauducbach.clone.modules.post.popularity.infrastructure.streams.serde.PostPopularitySerdes;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Uses forwarded brokers and only UUID-prefixed disposable topics/application state. */
@EnabledIfEnvironmentVariable(named = "POPULARITY_TEST_KAFKA_BOOTSTRAP", matches = ".+")
class PostPopularityKafkaIntegrationTest {
    @Test
    void committedPromotionSurvivesWorkerRestartAndDuplicatePublish() throws Exception {
        String bootstrap = System.getenv("POPULARITY_TEST_KAFKA_BOOTSTRAP");
        String prefix = "test-popularity-" + UUID.randomUUID();
        var properties = new PostPopularityProperties();
        properties.setThreshold(0);
        properties.setApplicationId(prefix);
        properties.setLikeTopic(prefix + "-likes"); properties.setCommentTopic(prefix + "-comments");
        properties.setInteractionTopic(prefix + "-views"); properties.setOutputTopic(prefix + "-promotions");
        properties.setInvalidTopic(prefix + "-invalid");
        var rawTopics = List.of(properties.getLikeTopic(), properties.getCommentTopic(), properties.getInteractionTopic(),
                properties.getOutputTopic(), properties.getInvalidTopic());
        Properties client = new Properties(); client.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(client)) {
            admin.createTopics(rawTopics.stream().map(topic -> new NewTopic(topic, 1, (short) 1)).toList()).all().get(15, TimeUnit.SECONDS);
            Properties streams = new Properties(); streams.putAll(client);
            streams.put(StreamsConfig.APPLICATION_ID_CONFIG, prefix);
            streams.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
            streams.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
            streams.put(StreamsConfig.STATE_DIR_CONFIG, java.nio.file.Files.createTempDirectory(prefix).toString());
            Properties producerConfig = new Properties(); producerConfig.putAll(client);
            producerConfig.put("key.serializer", StringSerializer.class); producerConfig.put("value.serializer", StringSerializer.class);
            producerConfig.put("acks", "all");
            Properties consumerConfig = new Properties(); consumerConfig.putAll(client);
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, prefix + "-verify");
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            KafkaStreams worker = new KafkaStreams(PostPopularityTopology.build(properties), streams);
            try (var producer = new KafkaProducer<String, String>(producerConfig); var consumer = new KafkaConsumer<String, String>(consumerConfig)) {
                consumer.subscribe(List.of(properties.getOutputTopic())); worker.start();
                Instant time = Instant.ofEpochMilli(Instant.now().toEpochMilli());
                String event = new PostInteractionEvent(UUID.randomUUID().toString(), "post-test", "actor-test", UUID.randomUUID().toString(), true, 0, 1, time).toJson().toString();
                producer.send(new ProducerRecord<>(properties.getInteractionTopic(), "post-test", event)).get(10, TimeUnit.SECONDS);
                PostPopularityUpdate promotion = null;
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (promotion == null && System.nanoTime() < deadline) {
                    for (var row : consumer.poll(Duration.ofMillis(500))) promotion = PostPopularitySerdes.read(row.value(), PostPopularityUpdate.class);
                }
                assertThat(promotion).isNotNull();
                assertThat(promotion.popularSince()).isEqualTo(time);
                worker.close(Duration.ofSeconds(10));
                worker = new KafkaStreams(PostPopularityTopology.build(properties), streams); worker.start();
                producer.send(new ProducerRecord<>(properties.getInteractionTopic(), "post-test", event)).get(10, TimeUnit.SECONDS);
                long quietUntil = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (System.nanoTime() < quietUntil) assertThat(consumer.poll(Duration.ofMillis(500)).isEmpty()).isTrue();
            } finally {
                worker.close(Duration.ofSeconds(10));
                var owned = admin.listTopics().names().get(10, TimeUnit.SECONDS).stream().filter(name -> name.startsWith(prefix + "-")).toList();
                admin.deleteTopics(owned).all().get(15, TimeUnit.SECONDS);
            }
        }
    }
}
