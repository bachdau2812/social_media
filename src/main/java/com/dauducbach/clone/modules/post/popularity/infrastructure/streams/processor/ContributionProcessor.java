package com.dauducbach.clone.modules.post.popularity.infrastructure.streams.processor;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.post.dto.event.PostEngagementEvent;
import com.dauducbach.clone.modules.post.popularity.infrastructure.streams.serde.PostPopularitySerdes;
import com.dauducbach.clone.modules.post.popularity.infrastructure.streams.state.ImpressionState;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;

public class ContributionProcessor extends ContextualProcessor<String, PostEngagementEvent, String, PostEngagementEvent> {
    public static final String FACTS = "post-contribution-facts-v1";
    public static final String EXPIRY = "post-contribution-expiry-v1";
    private final PostPopularityProperties properties;
    private KeyValueStore<String, String> facts;
    private KeyValueStore<String, String> expiry;

    public ContributionProcessor(PostPopularityProperties properties) { this.properties = properties; }

    @Override
    public void init(ProcessorContext<String, PostEngagementEvent> context) {
        super.init(context);
        facts = context.getStateStore(FACTS);
        expiry = context.getStateStore(EXPIRY);
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::cleanup);
    }

    @Override
    public void process(Record<String, PostEngagementEvent> record) {
        PostEngagementEvent event = record.value();
        long occurred = event.occurredAt().toEpochMilli();
        String identity = "D:" + component(event.kind(), event.postId(), event.actorId(), event.eventId());
        if (facts.get(identity) != null) return;
        save(identity, new ImpressionState(occurred, occurred, occurred + properties.getIdentityRetention().toMillis(), false, 0, 0));
        if (!event.kind().equals("VIEW")) {
            context().forward(record.withTimestamp(occurred));
            return;
        }
        String key = "I:" + component(event.postId(), event.actorId(), event.impressionId());
        String stored = facts.get(key);
        ImpressionState old = stored == null ? new ImpressionState(occurred, occurred, occurred, false, 0, 0)
                : PostPopularitySerdes.read(stored, ImpressionState.class);
        long first = Math.min(old.firstOccurredAt(), occurred);
        long last = Math.max(old.lastOccurredAt(), occurred);
        if (last - first > properties.getImpressionSpan().toMillis()) return;
        boolean click = old.clicked() || event.clicked();
        int view = Math.max(old.maxViewTime(), event.viewTime());
        int score = (click ? 1 : 0) + (view > 60 ? 2 : view > 30 ? 1 : 0);
        save(key, new ImpressionState(first, last, Math.max(old.expiresAt(), occurred + properties.getIdentityRetention().toMillis()),
                click, view, score));
        if (score > old.earnedScore()) context().forward(record.withValue(event.withWeight(score - old.earnedScore())).withTimestamp(occurred));
    }

    private String component(String... parts) {
        return java.util.Arrays.stream(parts).map(p -> Base64.getUrlEncoder().withoutPadding()
                .encodeToString(p.getBytes(StandardCharsets.UTF_8))).collect(java.util.stream.Collectors.joining("."));
    }

    private void save(String key, ImpressionState value) {
        String previous = facts.get(key);
        if (previous != null) expiry.delete(index(PostPopularitySerdes.read(previous, ImpressionState.class).expiresAt(), key));
        facts.put(key, PostPopularitySerdes.json(value));
        expiry.put(index(value.expiresAt(), key), key);
    }

    private String index(long time, String key) { return String.format(java.util.Locale.ROOT, "%019d:%s", time, key); }

    private void cleanup(long streamTime) {
        try (var rows = expiry.range("", index(streamTime, "~"))) {
            int remaining = 1000;
            while (rows.hasNext() && remaining-- > 0) {
                var row = rows.next();
                facts.delete(row.value);
                expiry.delete(row.key);
            }
        }
    }
}
