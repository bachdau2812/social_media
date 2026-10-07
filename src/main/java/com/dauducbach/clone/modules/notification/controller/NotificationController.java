package com.dauducbach.clone.modules.notification.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.security.ActorIdentity;
import com.dauducbach.clone.modules.notification.dto.request.PushTokenRegisterRequest;
import com.dauducbach.clone.modules.notification.dto.response.PushTokenRegisterResponse;
import com.dauducbach.clone.modules.notification.service.PushTokenRegistrationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/notifications")
public class NotificationController {
    private final PushTokenRegistrationService pushTokenRegistrationService;

    @PostMapping("/push-tokens")
    public Mono<ApiResponse<PushTokenRegisterResponse>> registerPushToken(
            @Valid @RequestBody PushTokenRegisterRequest request,
            Authentication authentication
    ) {
        ActorIdentity.require(authentication.getName(), request.userId());
        return pushTokenRegistrationService.registerPushToken(request)
                .map(response -> ApiResponse.<PushTokenRegisterResponse>builder()
                        .message("Push token registered successfully")
                        .result(response)
                        .build());
    }

    @DeleteMapping("/push-tokens")
    public Mono<ApiResponse<String>> removePushToken(
            @RequestParam String deviceId,
            Authentication authentication
    ) {
        String userId = ActorIdentity.require(authentication.getName(), null);
        return pushTokenRegistrationService.removePushToken(userId, deviceId)
                .thenReturn(ApiResponse.<String>builder()
                        .message("Push token removed successfully")
                        .result("removed")
                        .build());
    }
}
