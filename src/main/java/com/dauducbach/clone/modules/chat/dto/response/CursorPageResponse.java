package com.dauducbach.clone.modules.chat.dto.response;

import java.util.List;

public record CursorPageResponse<T>(List<T> items, String nextCursor, boolean hasMore) {
}
