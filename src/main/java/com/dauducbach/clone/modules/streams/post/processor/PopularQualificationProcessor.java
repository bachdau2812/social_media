package com.dauducbach.clone.modules.streams.post.processor;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.post.dto.event.PostPopularityUpdate;
import com.dauducbach.clone.modules.streams.post.serde.PostPopularitySerdes;
import com.dauducbach.clone.modules.streams.post.state.EngagementWindowState;
import com.dauducbach.clone.modules.streams.post.state.PopularityPeriodState;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.time.Duration;
import java.util.Locale;

public class PopularQualificationProcessor extends ContextualProcessor<String, EngagementWindowState, String, String> {
    public static final String PERIODS = "post-popularity-period-v1";
    public static final String EXPIRY = "post-popularity-period-expiry-v1";
    private final PostPopularityProperties properties;
    private KeyValueStore<String, String> periods;
    private KeyValueStore<String, String> expiry;

    public PopularQualificationProcessor(PostPopularityProperties properties) { this.properties = properties; }

    @Override
    public void init(ProcessorContext<String, String> context) {
        super.init(context);
        periods = context.getStateStore(PERIODS);
        expiry = context.getStateStore(EXPIRY);
        context.schedule(Duration.ofMinutes(1), PunctuationType.STREAM_TIME, this::cleanup);
    }

    @Override
    public void process(Record<String, EngagementWindowState> record) {
        EngagementWindowState candidate = record.value();
        if (candidate.score() <= properties.getThreshold()) return;
        long since = candidate.maxOccurredAt();
        String stored = periods.get(record.key());
        if (stored != null) {
            var previous = PostPopularitySerdes.read(stored, PopularityPeriodState.class);
            if (since <= previous.expiresAt() || candidate.triggerOccurredAt() <= previous.expiresAt()) return;
            expiry.delete(index(retainedUntil(previous), record.key()));
        }
        long expires = since + properties.getPopularLifetime().toMillis();
        // Retained period also fences older qualifying snapshots; no wall-clock reset on replay.
        var period = new PopularityPeriodState(since, expires);
        periods.put(record.key(), PostPopularitySerdes.json(period));
        expiry.put(index(retainedUntil(period), record.key()), record.key());
        PostPopularityUpdate update = new PostPopularityUpdate(1, "POPULAR_V1:" + record.key() + ":" + since,
                record.key(), Instant.ofEpochMilli(since), Instant.ofEpochMilli(expires), candidate.score(), "popular-v1");
        context().forward(record.withValue(PostPopularitySerdes.json(update)));
    }

    private long retainedUntil(PopularityPeriodState period) {
        return Math.max(period.popularSince() + properties.getIdentityRetention().toMillis(), period.expiresAt());
    }

    private String index(long time, String postId) {
        return String.format(Locale.ROOT, "%019d:%s", time, postId);
    }

    private void cleanup(long streamTime) {
        // The next timestamp also includes every postId at exactly streamTime, regardless of Unicode.
        try (var rows = expiry.range("", index(streamTime + 1, ""))) {
            int remaining = 1000;
            while (rows.hasNext() && remaining-- > 0) {
                var row = rows.next();
                String stored = periods.get(row.value);
                if (stored != null && retainedUntil(PostPopularitySerdes.read(stored, PopularityPeriodState.class)) <= streamTime) {
                    periods.delete(row.value);
                }
                expiry.delete(row.key);
            }
        }
    }
}
