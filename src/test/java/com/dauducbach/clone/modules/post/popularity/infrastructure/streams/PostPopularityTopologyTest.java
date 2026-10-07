package com.dauducbach.clone.modules.post.popularity.infrastructure.streams;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class PostPopularityTopologyTest {
    private static final Instant BASE = Instant.parse("2026-10-06T01:00:00Z");

    private TopologyTestDriver driver(int threshold) {
        return driver(threshold, Duration.ofHours(1));
    }

    private TopologyTestDriver driver(int threshold, Duration hop) {
        PostPopularityProperties config = new PostPopularityProperties();
        config.setThreshold(threshold);
        config.setHop(hop);
        Properties kafka = new Properties();
        kafka.put(StreamsConfig.APPLICATION_ID_CONFIG, "popularity-test");
        kafka.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        return new TopologyTestDriver(PostPopularityTopology.build(config,
                Clock.fixed(BASE.plus(Duration.ofDays(10)), ZoneOffset.UTC)), kafka, BASE);
    }

    @Test
    void mergesClickAndDwellFactsAndPromotesOnlyOnceAcrossOverlappingWindows() throws Exception {
        try (var driver = driver(2, Duration.ofMinutes(5))) {
            var input = driver.createInputTopic("post_interaction", new StringSerializer(), new StringSerializer());
            var output = driver.createOutputTopic("post_popularity_updates", new StringDeserializer(), new StringDeserializer());
            input.pipeInput("p1", view("e1", true, 0), BASE);
            assertThat(output.isEmpty()).isTrue();
            input.pipeInput("p1", view("e2", false, 61), BASE.plusSeconds(1));
            assertThat(output.getQueueSize()).isEqualTo(1);
            JsonNode promoted = new ObjectMapper().readTree(output.readValue());
            assertThat(promoted.path("qualificationScore").asLong()).isEqualTo(3);
            assertThat(promoted.path("expiresAt").asText()).isEqualTo(BASE.plusSeconds(1).plus(Duration.ofHours(48)).toString());
            input.pipeInput("p1", view("e2", false, 61), BASE.plusSeconds(1));
            input.pipeInput("p1", view("e3", false, 30), BASE.plusSeconds(2));
            assertThat(output.isEmpty()).isTrue();
        }
    }

    @Test
    void deduplicatesCanonicalSourceIdsAndKeepsDifferentPostsAtSameTimestamp() {
        try (var driver = driver(1)) {
            var likes = driver.createInputTopic("like_event", new StringSerializer(), new StringSerializer());
            var comments = driver.createInputTopic("comment_success_event", new StringSerializer(), new StringSerializer());
            var output = driver.createOutputTopic("post_popularity_updates", new StringDeserializer(), new StringDeserializer());
            likes.pipeInput("actor", like("LIKE:1", "p1", "POST"), BASE);
            likes.pipeInput("actor", like("LIKE:1", "p1", "POST"), BASE);
            assertThat(output.isEmpty()).isTrue();
            comments.pipeInput("other-actor", comment("COMMENT:1", "p1"), BASE);
            likes.pipeInput("actor", like("LIKE:2", "p2", "POST"), BASE);
            comments.pipeInput("other-actor", comment("COMMENT:2", "p2"), BASE);
            assertThat(output.readKeyValuesToList()).extracting(kv -> kv.key).containsExactlyInAnyOrder("p1", "p2");
        }
    }

    @Test
    void quarantinesMalformedLegacyEventsAndIgnoresCommentLikes() {
        try (var driver = driver(0)) {
            var likes = driver.createInputTopic("like_event", new StringSerializer(), new StringSerializer());
            var output = driver.createOutputTopic("post_popularity_updates", new StringDeserializer(), new StringDeserializer());
            var quarantine = driver.createOutputTopic("post_popularity_invalid", new StringDeserializer(), new StringDeserializer());
            likes.pipeInput("actor", "{bad-json", BASE);
            likes.pipeInput("actor", "{\"postId\":\"p1\"}", BASE);
            likes.pipeInput("actor", like("LIKE:comment", "p1", "COMMENT"), BASE);
            assertThat(quarantine.getQueueSize()).isEqualTo(2);
            assertThat(output.isEmpty()).isTrue();
        }
    }

    @Test
    void expiredPeriodRequiresNewActivityAndDoesNotResetExpiryOnReplay() {
        try (var driver = driver(0)) {
            var likes = driver.createInputTopic("like_event", new StringSerializer(), new StringSerializer());
            var output = driver.createOutputTopic("post_popularity_updates", new StringDeserializer(), new StringDeserializer());
            likes.pipeInput("actor", like("LIKE:1", "p1", "POST"), BASE);
            output.readValue();
            Instant later = BASE.plus(Duration.ofHours(49));
            String fresh = like("LIKE:2", "p1", "POST").replace(BASE.toString(), later.toString());
            likes.pipeInput("actor", fresh, later);
            assertThat(output.getQueueSize()).isEqualTo(1);
            output.readValue();
            likes.pipeInput("actor", like("LIKE:1", "p1", "POST"), BASE);
            assertThat(output.isEmpty()).isTrue();
        }
    }

    @Test
    void rejectsOlderReportWhenCombinedImpressionSpanExceedsTwoHours() {
        try (var driver = driver(1)) {
            var input = driver.createInputTopic("post_interaction", new StringSerializer(), new StringSerializer());
            var output = driver.createOutputTopic("post_popularity_updates", new StringDeserializer(), new StringDeserializer());
            Instant late = BASE.plus(Duration.ofHours(3));
            input.pipeInput("p1", view("e1", true, 0).replace(BASE.toString(), late.toString()), late);
            input.pipeInput("p1", view("e2", false, 61).replace(BASE.plusSeconds(1).toString(), BASE.toString()), BASE);
            assertThat(output.isEmpty()).isTrue();
        }
    }

    @Test
    void cleansPeriodReplayFenceAfterRetentionHorizon() {
        try (var driver = driver(0)) {
            var likes = driver.createInputTopic("like_event", new StringSerializer(), new StringSerializer());
            likes.pipeInput("actor", like("LIKE:1", "p1", "POST"), BASE);
            likes.pipeInput("actor", like("LIKE:expired", "expired", "POST"), BASE);
            var periods = driver.getKeyValueStore(com.dauducbach.clone.modules.post.popularity.infrastructure.streams.processor.PopularQualificationProcessor.PERIODS);
            assertThat(periods.get("p1")).isNotNull();
            Instant renewed = BASE.plus(Duration.ofHours(49));
            likes.pipeInput("actor", like("LIKE:renewal", "p1", "POST").replace(BASE.toString(), renewed.toString()), renewed);
            Instant later = BASE.plus(Duration.ofDays(9));
            likes.pipeInput("actor", like("LIKE:2", "p2", "POST").replace(BASE.toString(), later.toString()), later);
            assertThat(periods.get("expired")).isNull();
            assertThat(periods.get("p1")).isNotNull();
            assertThat(periods.get("p2")).isNotNull();
        }
    }

    @Test
    void mergesReverseOrderReportsWithinTwoHourSpan() {
        try (var driver = driver(2)) {
            var input = driver.createInputTopic("post_interaction", new StringSerializer(), new StringSerializer());
            var output = driver.createOutputTopic("post_popularity_updates", new StringDeserializer(), new StringDeserializer());
            Instant later = BASE.plus(Duration.ofHours(1));
            input.pipeInput("p1", view("e1", true, 0).replace(BASE.toString(), later.toString()), later);
            input.pipeInput("p1", view("e2", false, 61).replace(BASE.plusSeconds(1).toString(), BASE.toString()), BASE);
            assertThat(output.getQueueSize()).isEqualTo(1);
        }
    }

    @Test
    void latePreExpiryContributionCannotInitiateNewPeriod() {
        try (var driver = driver(1)) {
            var likes = driver.createInputTopic("like_event", new StringSerializer(), new StringSerializer());
            var output = driver.createOutputTopic("post_popularity_updates", new StringDeserializer(), new StringDeserializer());
            likes.pipeInput("actor", like("LIKE:1", "p1", "POST"), BASE);
            likes.pipeInput("actor", like("LIKE:2", "p1", "POST"), BASE);
            output.readValue();
            Instant fresh = BASE.plus(Duration.ofHours(48)).plusSeconds(60);
            Instant late = fresh.minusSeconds(120);
            likes.pipeInput("actor", like("LIKE:3", "p1", "POST").replace(BASE.toString(), fresh.toString()), fresh);
            likes.pipeInput("actor", like("LIKE:4", "p1", "POST").replace(BASE.toString(), late.toString()), late);
            assertThat(output.isEmpty()).isTrue();
            likes.pipeInput("actor", like("LIKE:5", "p1", "POST").replace(BASE.toString(), fresh.plusSeconds(1).toString()), fresh.plusSeconds(1));
            assertThat(output.getQueueSize()).isEqualTo(1);
        }
    }

    private String view(String id, boolean clicked, int dwell) {
        Instant occurred = BASE.plusSeconds(id.equals("e1") ? 0 : id.equals("e2") ? 1 : 2);
        return "{\"schemaVersion\":1,\"eventId\":\"" + id + "\",\"postId\":\"p1\",\"actorId\":\"a1\","
                + "\"impressionId\":\"i1\",\"isClick\":" + clicked + ",\"viewTime\":" + dwell
                + ",\"score\":" + ((clicked ? 1 : 0) + (dwell > 60 ? 2 : dwell > 30 ? 1 : 0))
                + ",\"occurredAt\":\"" + occurred + "\"}";
    }

    private String like(String id, String post, String type) {
        return "{\"eventId\":\"" + id + "\",\"postId\":\"" + post + "\",\"actorId\":\"a1\","
                + "\"targetType\":\"" + type + "\",\"occurredAt\":\"" + BASE + "\"}";
    }

    private String comment(String id, String post) {
        return "{\"eventId\":\"" + id + "\",\"postId\":\"" + post + "\",\"userId\":\"a2\","
                + "\"occurredAt\":\"" + BASE.minusSeconds(300) + "\",\"popularityOccurredAt\":\"" + BASE + "\"}";
    }
}
