package com.dauducbach.clone.modules.media.dto.music.response;

import com.dauducbach.clone.modules.media.constant.MusicFetchType;

import java.util.List;

public record BulkMusicFetchResponse(
        MusicFetchType type,
        int selectedCount,
        int startedCount,
        int processingCount,
        int alreadyFetchedCount,
        int failedCount,
        List<BulkMusicFetchItemResponse> items
) {
    public BulkMusicFetchResponse {
        items = List.copyOf(items);
    }

    public static BulkMusicFetchResponse from(
            MusicFetchType type,
            List<BulkMusicFetchItemResponse> items) {
        List<BulkMusicFetchItemResponse> snapshot = List.copyOf(items);
        return new BulkMusicFetchResponse(
                type,
                snapshot.size(),
                count(snapshot, BulkMusicFetchItemResponse.Status.STARTED),
                count(snapshot, BulkMusicFetchItemResponse.Status.PROCESSING),
                count(snapshot, BulkMusicFetchItemResponse.Status.ALREADY_FETCHED),
                count(snapshot, BulkMusicFetchItemResponse.Status.FAILED),
                snapshot);
    }

    private static int count(
            List<BulkMusicFetchItemResponse> items,
            BulkMusicFetchItemResponse.Status status) {
        return (int) items.stream().filter(item -> item.status() == status).count();
    }
}
