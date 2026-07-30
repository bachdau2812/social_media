package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.constant.UserActionType;
import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationContentNormalizerTest {
    private final NotificationContentNormalizer normalizer = new NotificationContentNormalizer();

    @Test
    void usesGroupNameForMemberRequestWhenAvailable() {
        NotificationForService notification = NotificationForService.builder()
                .actionType(UserActionType.CHAT_MEMBER_REQUEST)
                .htmlContent("legacy template")
                .metadata(Map.of("GROUP_NAME", "Backend"))
                .build();

        assertThat(normalizer.normalize(notification))
                .isEqualTo("Nhóm Backend của bạn có yêu cầu tham gia mới");
    }

    @Test
    void preservesExistingContentWhenGroupNameIsMissing() {
        NotificationForService notification = NotificationForService.builder()
                .actionType(UserActionType.CHAT_MEMBER_REQUEST)
                .htmlContent("Có yêu cầu tham gia mới")
                .metadata(Map.of())
                .build();

        assertThat(normalizer.normalize(notification)).isEqualTo("Có yêu cầu tham gia mới");
    }
}
