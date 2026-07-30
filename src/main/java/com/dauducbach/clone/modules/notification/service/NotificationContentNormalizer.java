package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class NotificationContentNormalizer {

    public String normalize(NotificationForService notification) {
        if (notification == null) {
            return "";
        }
        if (notification.getActionType() == UserActionType.CHAT_MEMBER_REQUEST) {
            String groupName = metadataValue(notification.getMetadata(), "GROUP_NAME");
            if (!groupName.isBlank()) {
                return "Nhóm " + groupName + " của bạn có yêu cầu tham gia mới";
            }
        }
        return notification.getHtmlContent() == null ? "" : notification.getHtmlContent();
    }

    private String metadataValue(Map<String, String> metadata, String key) {
        if (metadata == null) {
            return "";
        }
        return metadata.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getKey().equalsIgnoreCase(key))
                .map(Map.Entry::getValue)
                .filter(value -> value != null && !value.isBlank())
                .map(String::trim)
                .findFirst()
                .orElse("");
    }
}
