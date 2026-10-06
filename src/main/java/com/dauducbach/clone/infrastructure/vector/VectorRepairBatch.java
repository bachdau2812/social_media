package com.dauducbach.clone.infrastructure.vector;

import java.time.Duration;
import java.util.List;

/** Explicit operator-supplied inventory pages; never an unbounded whole-index scan. */
public final class VectorRepairBatch {
    public static final int MAX_IDS = 100;
    private VectorRepairBatch() {}

    public static List<String> ids(List<String> ids) {
        if (ids == null || ids.isEmpty() || ids.size() > MAX_IDS)
            throw new IllegalArgumentException("Repair batch requires 1..100 explicit IDs");
        if (ids.stream().anyMatch(id -> id == null || id.isBlank() || id.length() > 255 || !id.equals(id.trim())))
            throw new IllegalArgumentException("Repair IDs must be trimmed, nonblank and at most 255 characters");
        return ids.stream().distinct().toList();
    }

    public static Duration interval(Duration interval) {
        if (interval == null || interval.compareTo(Duration.ofMillis(250)) < 0 || interval.compareTo(Duration.ofSeconds(60)) > 0)
            throw new IllegalArgumentException("Repair interval must be 250ms..60s (at most four starts/second)");
        return interval;
    }
}
