package com.dauducbach.clone.modules.media.publicapi;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;

public interface MediaUrlDelivery {
    String transformDeliveryUrl(String mediaUrl, MediaDisplayType displayType);
    String storyVideoStill(String mediaUrl, long previewAtMs);
}
