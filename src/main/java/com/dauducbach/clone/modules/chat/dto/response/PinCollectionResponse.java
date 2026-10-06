package com.dauducbach.clone.modules.chat.dto.response;
import java.util.List;
public record PinCollectionResponse(long version, boolean canManage, List<PinnedMessageResponse> items) {
    public PinCollectionResponse { items = List.copyOf(items); }
}
