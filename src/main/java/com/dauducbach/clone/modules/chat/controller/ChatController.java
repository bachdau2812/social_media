package com.dauducbach.clone.modules.chat.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.security.ActorIdentity;
import com.dauducbach.clone.modules.chat.dto.request.AddConversationMemberRequest;
import com.dauducbach.clone.modules.chat.dto.request.ChangeMemberRoleRequest;
import com.dauducbach.clone.modules.chat.dto.request.CreateDirectConversationRequest;
import com.dauducbach.clone.modules.chat.dto.request.CreateGroupConversationRequest;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.request.UpdateChatCursorRequest;
import com.dauducbach.clone.modules.chat.dto.request.UpdateConversationNotificationRequest;
import com.dauducbach.clone.modules.chat.dto.request.UpdateMemberNicknameRequest;
import com.dauducbach.clone.modules.chat.dto.response.ChatCursorResponse;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.ChatPresenceResponse;
import com.dauducbach.clone.modules.chat.dto.response.ConversationResponse;
import com.dauducbach.clone.modules.chat.dto.response.ConversationDetailsResponse;
import com.dauducbach.clone.modules.chat.dto.response.ConversationNotificationResponse;
import com.dauducbach.clone.modules.chat.dto.response.CursorPageResponse;
import com.dauducbach.clone.modules.chat.dto.response.MemberMutationResponse;
import com.dauducbach.clone.modules.chat.dto.response.MemberNicknameResponse;
import com.dauducbach.clone.modules.chat.dto.response.MemberRequestPageResponse;
import com.dauducbach.clone.modules.chat.dto.response.ChatMediaResponse;
import com.dauducbach.clone.modules.chat.constant.ChatMediaCategory;
import com.dauducbach.clone.modules.chat.service.ChatCursorService;
import com.dauducbach.clone.modules.chat.service.ChatMessageQueryService;
import com.dauducbach.clone.modules.chat.service.ChatPresenceService;
import com.dauducbach.clone.modules.chat.service.ConversationMemberService;
import com.dauducbach.clone.modules.chat.service.ConversationService;
import com.dauducbach.clone.modules.chat.service.ConversationMediaQueryService;
import com.dauducbach.clone.modules.chat.service.SendMessageService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/chat")
public class ChatController {

    private final ConversationService conversationService;
    private final SendMessageService sendMessageService;
    private final ChatMessageQueryService messageQueryService;
    private final ChatCursorService cursorService;
    private final ConversationMemberService memberService;
    private final ChatPresenceService presenceService;
    private final ConversationMediaQueryService conversationMediaQueryService;

    @GetMapping("/presence/{userId}")
    public Mono<ApiResponse<ChatPresenceResponse>> getPresence(@PathVariable String userId) {
        return presenceService.getPresence(userId)
                .map(presence -> ApiResponse.<ChatPresenceResponse>builder()
                        .message("Chat presence fetched")
                        .result(presence)
                        .build());
    }

    @PostMapping("/conversations/direct")
    public Mono<ApiResponse<ConversationResponse>> createDirect(
            @RequestParam String actorId,
            Authentication authentication,
            @Valid @RequestBody CreateDirectConversationRequest request
    ) {
        return conversationService.createDirect(requireActor(authentication, actorId), request)
                .map(response -> ApiResponse.<ConversationResponse>builder()
                        .message("Direct conversation ready")
                        .result(response)
                        .build());
    }

    @PostMapping("/conversations/group")
    public Mono<ApiResponse<ConversationResponse>> createGroup(
            @RequestParam String actorId,
            Authentication authentication,
            @Valid @RequestBody CreateGroupConversationRequest request
    ) {
        return conversationService.createGroup(requireActor(authentication, actorId), request)
                .map(response -> ApiResponse.<ConversationResponse>builder()
                        .message("Group conversation created")
                        .result(response)
                        .build());
    }

    @GetMapping("/conversations")
    public Mono<ApiResponse<CursorPageResponse<ConversationResponse>>> getConversations(
            @RequestParam String actorId,
            Authentication authentication,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "50") int limit
    ) {
        return conversationService.getConversations(requireActor(authentication, actorId), cursor, limit)
                .map(response -> ApiResponse.<CursorPageResponse<ConversationResponse>>builder()
                        .message("Chat conversations fetched")
                        .result(response)
                        .build());
    }

    @GetMapping("/conversations/{conversationId}")
    public Mono<ApiResponse<ConversationResponse>> getConversation(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId
    ) {
        return conversationService.getConversation(requireActor(authentication, actorId), conversationId)
                .map(response -> ApiResponse.<ConversationResponse>builder()
                        .message("Chat conversation fetched")
                        .result(response)
                        .build());
    }

    @GetMapping("/conversations/{conversationId}/details")
    public Mono<ApiResponse<ConversationDetailsResponse>> getConversationDetails(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId
    ) {
        return memberService.getDetails(requireActor(authentication, actorId), conversationId)
                .map(response -> ApiResponse.<ConversationDetailsResponse>builder()
                        .message("Conversation details fetched")
                        .result(response)
                        .build());
    }

    @PutMapping("/conversations/{conversationId}/notifications")
    public Mono<ApiResponse<ConversationNotificationResponse>> updateConversationNotifications(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @RequestBody UpdateConversationNotificationRequest request
    ) {
        return memberService.updateNotificationSetting(requireActor(authentication, actorId), conversationId, request)
                .map(response -> ApiResponse.<ConversationNotificationResponse>builder()
                        .message("Conversation notification setting updated")
                        .result(response)
                        .build());
    }

    @PostMapping("/conversations/{conversationId}/messages")
    public Mono<ApiResponse<ChatMessageResponse>> sendMessage(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @Valid @RequestBody SendMessageRequest request
    ) {
        return sendMessageService.sendMessage(requireActor(authentication, actorId), conversationId, request)
                .map(response -> ApiResponse.<ChatMessageResponse>builder()
                        .message("Chat message sent")
                        .result(response)
                        .build());
    }

    @GetMapping("/conversations/{conversationId}/messages")
    public Mono<ApiResponse<CursorPageResponse<ChatMessageResponse>>> getMessages(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @RequestParam(required = false) Long afterSeq,
            @RequestParam(required = false) Long beforeSeq,
            @RequestParam(defaultValue = "50") int limit
    ) {
        return messageQueryService.getMessages(requireActor(authentication, actorId), conversationId, afterSeq, beforeSeq, limit)
                .map(response -> ApiResponse.<CursorPageResponse<ChatMessageResponse>>builder()
                        .message("Chat messages fetched")
                        .result(response)
                        .build());
    }

    @PutMapping("/conversations/{conversationId}/cursor/delivered")
    public Mono<ApiResponse<ChatCursorResponse>> markDelivered(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @Valid @RequestBody UpdateChatCursorRequest request
    ) {
        return cursorService.markDelivered(requireActor(authentication, actorId), conversationId, request.sequence())
                .map(response -> ApiResponse.<ChatCursorResponse>builder()
                        .message("Chat delivered cursor updated")
                        .result(response)
                        .build());
    }

    @PutMapping("/conversations/{conversationId}/cursor/read")
    public Mono<ApiResponse<ChatCursorResponse>> markRead(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @Valid @RequestBody UpdateChatCursorRequest request
    ) {
        return cursorService.markRead(requireActor(authentication, actorId), conversationId, request.sequence())
                .map(response -> ApiResponse.<ChatCursorResponse>builder()
                        .message("Chat read cursor updated")
                        .result(response)
                        .build());
    }

    @PutMapping("/conversations/{conversationId}/members/me/nickname")
    public Mono<ApiResponse<MemberNicknameResponse>> updateNickname(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @Valid @RequestBody UpdateMemberNicknameRequest request
    ) {
        return memberService.updateNickname(requireActor(authentication, actorId), conversationId, request)
                .map(response -> ApiResponse.<MemberNicknameResponse>builder()
                        .message("Chat nickname updated")
                        .result(response)
                        .build());
    }

    @PutMapping("/conversations/{conversationId}/members/{targetUserId}/nickname")
    public Mono<ApiResponse<MemberNicknameResponse>> updateMemberNickname(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @PathVariable String targetUserId,
            @Valid @RequestBody UpdateMemberNicknameRequest request
    ) {
        return memberService.updateNickname(requireActor(authentication, actorId), conversationId, targetUserId, request)
                .map(response -> ApiResponse.<MemberNicknameResponse>builder()
                        .message("Chat member nickname updated")
                        .result(response)
                        .build());
    }
    @PostMapping("/conversations/{conversationId}/members")
    public Mono<ApiResponse<MemberMutationResponse>> addMember(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @Valid @RequestBody AddConversationMemberRequest request
    ) {
        return memberService.addMember(requireActor(authentication, actorId), conversationId, request)
                .map(response -> ApiResponse.<MemberMutationResponse>builder()
                        .message("Chat member mutation accepted")
                        .result(response)
                        .build());
    }

    @DeleteMapping("/conversations/{conversationId}/members/{targetUserId}")
    public Mono<ApiResponse<MemberMutationResponse>> removeMember(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @PathVariable String targetUserId
    ) {
        return memberService.removeMember(requireActor(authentication, actorId), conversationId, targetUserId)
                .map(response -> ApiResponse.<MemberMutationResponse>builder()
                        .message("Group member removed")
                        .result(response)
                        .build());
    }

    @PatchMapping("/conversations/{conversationId}/members/{targetUserId}/role")
    public Mono<ApiResponse<MemberMutationResponse>> changeMemberRole(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @PathVariable String targetUserId,
            @Valid @RequestBody ChangeMemberRoleRequest request
    ) {
        return memberService.changeMemberRole(requireActor(authentication, actorId), conversationId, targetUserId, request)
                .map(response -> ApiResponse.<MemberMutationResponse>builder()
                        .message("Group member role updated")
                        .result(response)
                        .build());
    }
    @GetMapping("/conversations/{conversationId}/member-requests")
    public Mono<ApiResponse<MemberRequestPageResponse>> getMemberRequests(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return memberService.getPendingRequests(requireActor(authentication, actorId), conversationId, page, size)
                .map(response -> ApiResponse.<MemberRequestPageResponse>builder()
                        .message("Chat member requests fetched")
                        .result(response)
                        .build());
    }

    @GetMapping("/conversations/{conversationId}/media")
    public Mono<ApiResponse<CursorPageResponse<ChatMediaResponse>>> getConversationMedia(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId,
            @RequestParam(defaultValue = "IMAGE") ChatMediaCategory category,
            @RequestParam(required = false) Long beforeSeq,
            @RequestParam(defaultValue = "30") int limit
    ) {
        return conversationMediaQueryService.getMedia(requireActor(authentication, actorId), conversationId, category, beforeSeq, limit)
                .map(response -> ApiResponse.<CursorPageResponse<ChatMediaResponse>>builder()
                        .message("Conversation media fetched")
                        .result(response)
                        .build());
    }

    @PostMapping("/conversations/{conversationId}/members/me/leave")
    public Mono<ApiResponse<MemberMutationResponse>> leaveConversation(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId
    ) {
        return memberService.leaveConversation(requireActor(authentication, actorId), conversationId)
                .map(response -> ApiResponse.<MemberMutationResponse>builder()
                        .message("Left group conversation")
                        .result(response)
                        .build());
    }
    @DeleteMapping("/conversations/{conversationId}/for-me")
    public Mono<ApiResponse<MemberMutationResponse>> deleteConversationForMe(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId
    ) {
        return memberService.deleteDirectForMe(requireActor(authentication, actorId), conversationId)
                .map(response -> ApiResponse.<MemberMutationResponse>builder()
                        .message("Direct conversation deleted for member")
                        .result(response)
                        .build());
    }

    @PostMapping("/conversations/{conversationId}/dissolve")
    public Mono<ApiResponse<MemberMutationResponse>> dissolveConversation(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String conversationId
    ) {
        return memberService.dissolveConversation(requireActor(authentication, actorId), conversationId)
                .map(response -> ApiResponse.<MemberMutationResponse>builder()
                        .message("Group conversation dissolved")
                        .result(response)
                        .build());
    }
    @PostMapping("/member-requests/{requestId}/approve")
    public Mono<ApiResponse<MemberMutationResponse>> approveRequest(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String requestId
    ) {
        return memberService.approveRequest(requireActor(authentication, actorId), requestId)
                .map(response -> ApiResponse.<MemberMutationResponse>builder()
                        .message("Chat member request approved")
                        .result(response)
                        .build());
    }

    @PostMapping("/member-requests/{requestId}/reject")
    public Mono<ApiResponse<MemberMutationResponse>> rejectRequest(
            @RequestParam String actorId,
            Authentication authentication,
            @PathVariable String requestId
    ) {
        return memberService.rejectRequest(requireActor(authentication, actorId), requestId)
                .map(response -> ApiResponse.<MemberMutationResponse>builder()
                        .message("Chat member request rejected")
                        .result(response)
                        .build());
    }
    private String requireActor(Authentication authentication, String actorId) {
        return ActorIdentity.require(authentication.getName(), actorId);
    }
}
