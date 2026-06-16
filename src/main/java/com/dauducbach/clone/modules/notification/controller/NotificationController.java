package com.dauducbach.clone.modules.notification.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.notification.dto.request.PushTokenRegisterRequest;
import com.dauducbach.clone.modules.notification.dto.response.PushTokenRegisterResponse;
import com.dauducbach.clone.modules.notification.service.PushNotificationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/notifications")
public class NotificationController {
    private final PushNotificationService pushNotificationService;

    @PostMapping("/push-tokens")
    public Mono<ApiResponse<PushTokenRegisterResponse>> registerPushToken(
            @Valid @RequestBody PushTokenRegisterRequest request
    ) {
        return pushNotificationService.registerPushToken(request)
                .map(response -> ApiResponse.<PushTokenRegisterResponse>builder()
                        .message("Push token registered successfully")
                        .result(response)
                        .build());
    }
}
