package com.dauducbach.clone.modules.feed.dto;

import java.util.List;

public record FeedVectorSnapshot(long version, List<Double> queryVector) {
    public FeedVectorSnapshot { queryVector = List.copyOf(queryVector); }
}
