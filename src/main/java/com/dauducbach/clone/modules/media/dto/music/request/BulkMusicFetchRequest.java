package com.dauducbach.clone.modules.media.dto.music.request;

import com.dauducbach.clone.modules.media.constant.MusicFetchType;

import java.util.List;

public record BulkMusicFetchRequest(
        MusicFetchType type,
        List<String> fetchList,
        Integer limit
) {
    public static final int DEFAULT_LIMIT = 20;
    public static final int MAX_LIMIT = 100;

    public BulkMusicFetchRequest {
        fetchList = fetchList == null ? List.of() : List.copyOf(fetchList);
    }

    public static BulkMusicFetchRequest top(int limit) {
        return new BulkMusicFetchRequest(MusicFetchType.TOP, List.of(), limit);
    }
}
