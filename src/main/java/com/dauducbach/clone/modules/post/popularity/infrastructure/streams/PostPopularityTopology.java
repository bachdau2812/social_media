package com.dauducbach.clone.modules.post.popularity.infrastructure.streams;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.post.dto.event.PostEngagementEvent;
import com.dauducbach.clone.modules.post.popularity.infrastructure.streams.processor.ContributionProcessor;
import com.dauducbach.clone.modules.post.popularity.infrastructure.streams.processor.PopularQualificationProcessor;
import com.dauducbach.clone.modules.post.popularity.infrastructure.streams.serde.PostPopularitySerdes;
import com.dauducbach.clone.modules.post.popularity.infrastructure.streams.state.EngagementWindowState;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;

import java.time.Clock;

public final class PostPopularityTopology {
    public static final String OUTPUT_TOPIC = "post_popularity_updates";
    public static final String INVALID_TOPIC = "post_popularity_invalid";

    private PostPopularityTopology() {}

    public static Topology build(PostPopularityProperties properties) { return build(properties, Clock.systemUTC()); }

    public static Topology build(PostPopularityProperties properties, Clock clock) {
        StreamsBuilder builder = new StreamsBuilder();
        for (String store : new String[]{ContributionProcessor.FACTS, ContributionProcessor.EXPIRY,
                PopularQualificationProcessor.PERIODS, PopularQualificationProcessor.EXPIRY}) {
            builder.addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(store), Serdes.String(), Serdes.String()));
        }
        EngagementNormalizer normalizer = new EngagementNormalizer(clock);
        KStream<String, PostEngagementEvent> likes = source(builder, normalizer, properties.getLikeTopic(), "LIKE", properties.getInvalidTopic());
        KStream<String, PostEngagementEvent> comments = source(builder, normalizer, properties.getCommentTopic(), "COMMENT", properties.getInvalidTopic());
        KStream<String, PostEngagementEvent> views = source(builder, normalizer, properties.getInteractionTopic(), "VIEW", properties.getInvalidTopic());
        var eventSerde = PostPopularitySerdes.of(PostEngagementEvent.class);
        var aggregateSerde = PostPopularitySerdes.of(EngagementWindowState.class);
        likes.merge(comments).merge(views)
                .selectKey((key, event) -> event.postId())
                .repartition(Repartitioned.<String, PostEngagementEvent>as("post-engagement-v1")
                        .withNumberOfPartitions(properties.getPartitions()).withKeySerde(Serdes.String()).withValueSerde(eventSerde))
                .process(() -> new ContributionProcessor(properties), Named.as("post-contributions-v1"), ContributionProcessor.FACTS, ContributionProcessor.EXPIRY)
                .groupByKey(Grouped.with(Serdes.String(), eventSerde))
                .windowedBy(TimeWindows.ofSizeAndGrace(properties.getWindowSize(), properties.getGrace()).advanceBy(properties.getHop()))
                .aggregate(() -> new EngagementWindowState(0, 0, 0),
                        (key, event, old) -> new EngagementWindowState(Math.addExact(old.score(), event.weight()),
                                Math.max(old.maxOccurredAt(), event.occurredAt().toEpochMilli()), event.occurredAt().toEpochMilli()),
                        Materialized.<String, EngagementWindowState, org.apache.kafka.streams.state.WindowStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("post-engagement-window-v1")
                                .withKeySerde(Serdes.String()).withValueSerde(aggregateSerde)
                                .withRetention(properties.getWindowRetention()).withCachingDisabled())
                .toStream().selectKey((windowed, aggregate) -> windowed.key())
                .repartition(Repartitioned.<String, EngagementWindowState>as("post-qualification-v1")
                        .withNumberOfPartitions(properties.getPartitions()).withKeySerde(Serdes.String()).withValueSerde(aggregateSerde))
                .process(() -> new PopularQualificationProcessor(properties), Named.as("post-qualification-gate-v1"),
                        PopularQualificationProcessor.PERIODS, PopularQualificationProcessor.EXPIRY)
                .to(properties.getOutputTopic(), Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static KStream<String, PostEngagementEvent> source(StreamsBuilder builder, EngagementNormalizer normalizer, String topic, String kind, String invalidTopic) {
        KStream<String, EngagementNormalizer.Parsed> parsed = builder.stream(topic,
                        Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(normalizer.extractor(kind)))
                .mapValues((key, raw) -> normalizer.parse(kind, key, raw));
        parsed.filter((key, value) -> value.error() != null)
                .mapValues(PostPopularitySerdes::json).to(invalidTopic, Produced.with(Serdes.String(), Serdes.String()));
        return parsed.filter((key, value) -> value.event() != null).mapValues(EngagementNormalizer.Parsed::event);
    }
}
