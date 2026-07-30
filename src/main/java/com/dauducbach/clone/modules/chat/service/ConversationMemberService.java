package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.auth.service.UserAccountQueryService;
import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MemberRequestStatus;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MemberStatus;
import com.dauducbach.clone.modules.chat.constant.SystemMessageAction;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.request.AddConversationMemberRequest;
import com.dauducbach.clone.modules.chat.dto.request.ChangeMemberRoleRequest;
import com.dauducbach.clone.modules.chat.dto.request.UpdateConversationNotificationRequest;
import com.dauducbach.clone.modules.chat.dto.request.UpdateMemberNicknameRequest;
import com.dauducbach.clone.modules.chat.dto.response.ConversationDetailsResponse;
import com.dauducbach.clone.modules.chat.dto.response.ConversationNotificationResponse;
import com.dauducbach.clone.modules.chat.dto.response.MemberMutationResponse;
import com.dauducbach.clone.modules.chat.dto.response.MemberNicknameResponse;
import com.dauducbach.clone.modules.chat.dto.response.MemberRequestPageResponse;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.entity.ConversationMemberRequest;
import com.dauducbach.clone.modules.chat.repository.ConversationDetailsRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRequestRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import com.dauducbach.clone.modules.chat.repository.MemberRequestQueryRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class ConversationMemberService {
    private static final Logger log = LoggerFactory.getLogger(ConversationMemberService.class);
    private static final int MAX_REQUEST_PAGE_SIZE = 50;

    ChatAccessService accessService;
    ConversationRepository conversationRepository;
    ConversationMemberRepository memberRepository;
    ConversationMemberRequestRepository requestRepository;
    MemberRequestQueryRepository memberRequestQueryRepository;
    ConversationDetailsRepository conversationDetailsRepository;
    UserAccountQueryService userAccountQueryService;
    ChatSystemMessageService systemMessageService;
    ChatEventPublisher eventPublisher;
    TransactionalOperator transactionalOperator;
    R2dbcEntityTemplate r2dbcEntityTemplate;

    public Mono<ConversationDetailsResponse> getDetails(String actorId, String conversationId) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        return accessService.requireActiveMember(id, actor)
                .flatMap(actorMember -> conversationRepository.findById(id)
                        .switchIfEmpty(Mono.error(new AppException(ErrorCode.CONVERSATION_NOT_FOUND, "Conversation not found")))
                        .flatMap(conversation -> conversationDetailsRepository.findActiveMembers(id, actor)
                                .collectList()
                                .map(members -> new ConversationDetailsResponse(
                                        id,
                                        actorMember.getMutedUntil() != null && actorMember.getMutedUntil().isAfter(Instant.now()),
                                        conversation.getCreatedBy(),
                                        conversation.getConversationType() == ConversationType.GROUP
                                                && actorMember.getMemberRole() == MemberRole.ADMIN,
                                        conversation.isDissolved(),
                                        members))))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CONVERSATION_FETCH_FAILED, "Fetch conversation details failed", error));
    }

    public Mono<MemberRequestPageResponse> getPendingRequests(String actorId, String conversationId, int page, int size) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        int pageNumber = Math.max(0, page);
        int pageSize = size <= 0 ? 20 : Math.min(size, MAX_REQUEST_PAGE_SIZE);
        long offset = (long) pageNumber * pageSize;
        return accessService.requireAdmin(id, actor)
                .then(Mono.zip(
                        memberRequestQueryRepository.findPending(id, pageSize, offset).collectList(),
                        memberRequestQueryRepository.countPending(id)))
                .map(tuple -> new MemberRequestPageResponse(
                        tuple.getT1(), pageNumber, pageSize, tuple.getT2(), offset + tuple.getT1().size() < tuple.getT2()));
    }

    public Mono<ConversationNotificationResponse> updateNotificationSetting(
            String actorId, String conversationId, UpdateConversationNotificationRequest request
    ) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        if (request == null) return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Notification setting is required"));
        Mono<Integer> update = request.muted()
                ? memberRepository.updateMutedUntil(id, actor, Instant.parse("2099-12-31T23:59:59Z"))
                : memberRepository.clearMutedUntil(id, actor);
        return accessService.requireActiveMember(id, actor)
                .then(update)
                .flatMap(updated -> updated > 0
                        ? Mono.just(new ConversationNotificationResponse(id, request.muted()))
                        : Mono.error(new AppException(ErrorCode.CHAT_MEMBER_UPDATE_FAILED, "Conversation notification setting was not updated")));
    }

    public Mono<MemberNicknameResponse> updateNickname(String actorId, String conversationId, UpdateMemberNicknameRequest request) {
        return updateNickname(actorId, conversationId, actorId, request);
    }

    public Mono<MemberNicknameResponse> updateNickname(
            String actorId, String conversationId, String targetUserId, UpdateMemberNicknameRequest request
    ) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        String target = requireIdentifier(targetUserId, "targetUserId");
        if (request == null) return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Nickname request is required"));
        String normalized = request.nickname() == null || request.nickname().trim().isEmpty() ? null : request.nickname().trim();
        Mono<Integer> update = normalized == null
                ? memberRepository.clearNickname(id, target)
                : memberRepository.updateNickname(id, target, normalized);
        Mono<NicknameOutcome> work = accessService.requireActiveMember(id, actor)
                .then(accessService.requireActiveMember(id, target))
                .then(conversationRepository.findByIdForUpdate(id))
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CONVERSATION_NOT_FOUND, "Conversation not found")))
                .flatMap(conversation -> {
                    ensureWritable(conversation);
                    return update;
                })
                .flatMap(updated -> updated > 0
                        ? systemMessageService.insert(id, actor, SystemMessageAction.NICKNAME_CHANGED, target, normalized)
                                .map(system -> new NicknameOutcome(new MemberNicknameResponse(id, target, normalized), system))
                        : Mono.error(new AppException(ErrorCode.CHAT_MEMBER_UPDATE_FAILED, "Conversation member nickname was not updated")));
        return transactionalOperator.transactional(work)
                .flatMap(outcome -> publishSystem(outcome.system()).thenReturn(outcome.response()));
    }

    public Mono<MemberMutationResponse> addMember(String actorId, String conversationId, AddConversationMemberRequest request) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        if (request == null) return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Add member request is required"));
        String target = requireIdentifier(request.targetUserId(), "targetUserId");
        if (actor.equals(target)) return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "targetUserId must be different from actorId"));

        Mono<MemberOutcome> work = ensureUserExists(target)
                .then(conversationRepository.findByIdForUpdate(id))
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CONVERSATION_NOT_FOUND, "Conversation not found")))
                .flatMap(conversation -> {
                    if (conversation.getConversationType() != ConversationType.GROUP) {
                        return Mono.error(new AppException(
                                ErrorCode.CHAT_REQUEST_INVALID, "Members can only be added to group conversations"));
                    }
                    ensureWritable(conversation);
                    long joinedSequence = conversation.getLastMessageSeq() + 1L;
                    return memberRepository.findMembershipForUpdate(id, actor)
                            .filter(member -> member.getMemberStatus() == MemberStatus.ACTIVE)
                            .switchIfEmpty(Mono.error(new AppException(
                                    ErrorCode.CONVERSATION_FORBIDDEN, "Active chat membership is required")))
                            .flatMap(actorMember -> memberRepository.findMembershipForUpdate(id, target)
                                    .flatMap(existing -> addOrRequestMember(
                                            id, actor, target, joinedSequence, actorMember, existing))
                                    .switchIfEmpty(Mono.defer(() -> addOrRequestMember(
                                            id, actor, target, joinedSequence, actorMember, null))));
                });
        return transactionalOperator.transactional(work)
                .flatMap(this::publishMemberOutcome)
                .onErrorMap(error -> error instanceof AppException ? error
                        : new AppException(ErrorCode.CHAT_MEMBER_UPDATE_FAILED, "Add chat member failed", error));
    }

    private Mono<MemberOutcome> addOrRequestMember(
            String conversationId,
            String actorId,
            String targetUserId,
            long joinedSequence,
            ConversationMember actorMember,
            ConversationMember existing
    ) {
        if (existing != null && existing.getMemberStatus() == MemberStatus.ACTIVE) {
            return Mono.error(new AppException(
                    ErrorCode.CHAT_MEMBER_ALREADY_EXISTS, "Target user is already an active member"));
        }
        if (existing != null && existing.getMemberStatus() == MemberStatus.BANNED) {
            return Mono.error(new AppException(
                    ErrorCode.CONVERSATION_FORBIDDEN, "Banned member cannot be re-added"));
        }
        if (actorMember.getMemberRole() != MemberRole.ADMIN) {
            return createPendingOutcome(conversationId, actorId, targetUserId);
        }
        return activateMember(conversationId, targetUserId, joinedSequence, existing)
                .flatMap(response -> systemMessageService.insert(
                                conversationId, actorId, SystemMessageAction.MEMBER_ADDED, targetUserId, null)
                        .map(system -> MemberOutcome.system(
                                response,
                                system,
                                ChatEvent.memberAdded(conversationId, actorId, targetUserId))));
    }

    public Mono<MemberMutationResponse> approveRequest(String actorId, String requestId) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(requestId, "requestId");
        Mono<MemberOutcome> work = requestRepository.findByIdForUpdate(id)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CHAT_MEMBER_REQUEST_NOT_FOUND, "Member request was not found")))
                .flatMap(request -> requireAdminForUpdate(request.getConversationId(), actor)
                        .flatMap(conversation -> {
                            ensureWritable(conversation);
                            return resolveApprove(actor, request, conversation.getLastMessageSeq() + 1L);
                        })
                        .flatMap(response -> systemMessageService.insert(
                                        request.getConversationId(), actor, SystemMessageAction.MEMBER_ADDED, request.getTargetUserId(), null)
                                .map(system -> MemberOutcome.system(
                                        response,
                                        system,
                                        ChatEvent.memberAdded(
                                                request.getConversationId(), actor, request.getTargetUserId())))));
        return transactionalOperator.transactional(work).flatMap(this::publishMemberOutcome);
    }
    public Mono<MemberMutationResponse> rejectRequest(String actorId, String requestId) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(requestId, "requestId");
        Mono<MemberMutationResponse> work = requestRepository.findByIdForUpdate(id)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CHAT_MEMBER_REQUEST_NOT_FOUND, "Member request was not found")))
                .flatMap(request -> requireAdminForUpdate(request.getConversationId(), actor)
                        .flatMap(conversation -> {
                            ensureWritable(conversation);
                            return resolveReject(actor, request);
                        }));
        return transactionalOperator.transactional(work);
    }
    public Mono<MemberMutationResponse> deleteDirectForMe(String actorId, String conversationId) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        Mono<MemberMutationResponse> work = accessService.requireActiveMember(id, actor)
                .then(conversationRepository.findByIdForUpdate(id))
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CONVERSATION_NOT_FOUND, "Conversation not found")))
                .flatMap(conversation -> {
                    if (conversation.getConversationType() != ConversationType.DIRECT) {
                        return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Only direct conversations can be deleted for one member"));
                    }
                    return memberRepository.advanceDeleteBoundary(id, actor, conversation.getLastMessageSeq())
                            .flatMap(updated -> updated > 0
                                    ? Mono.just(new MemberMutationResponse(id, actor, "DELETED_FOR_ME", null))
                                    : Mono.error(new AppException(ErrorCode.CHAT_MEMBER_UPDATE_FAILED, "Delete direct conversation failed")));
                });
        return transactionalOperator.transactional(work);
    }

    public Mono<MemberMutationResponse> dissolveConversation(String actorId, String conversationId) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        Mono<MemberOutcome> work = requireAdminForUpdate(id, actor)
                .flatMap(conversation -> {
                    if (conversation.isDissolved()) {
                        return Mono.just(MemberOutcome.plain(new MemberMutationResponse(id, actor, "DISSOLVED", null)));
                    }
                    return systemMessageService.insert(id, actor, SystemMessageAction.GROUP_DISSOLVED, null, null)
                            .flatMap(system -> conversationRepository.markDissolved(id, Instant.now())
                                    .flatMap(updated -> updated > 0
                                            ? Mono.just(MemberOutcome.system(new MemberMutationResponse(id, actor, "DISSOLVED", null), system))
                                            : Mono.error(new AppException(ErrorCode.CHAT_MEMBER_UPDATE_FAILED, "Dissolve group failed"))));
                });
        return transactionalOperator.transactional(work).flatMap(this::publishMemberOutcome);
    }

    public Mono<MemberMutationResponse> removeMember(String actorId, String conversationId, String targetUserId) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        String target = requireIdentifier(targetUserId, "targetUserId");
        if (actor.equals(target)) {
            return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Use the leave endpoint to remove yourself"));
        }
        Mono<MemberOutcome> work = requireAdminForUpdate(id, actor)
                .flatMap(conversation -> {
                    ensureWritable(conversation);
                    return memberRepository.findMembershipForUpdate(id, target)
                            .filter(member -> member.getMemberStatus() == MemberStatus.ACTIVE)
                            .switchIfEmpty(Mono.error(new AppException(ErrorCode.CHAT_MEMBER_NOT_FOUND, "Active target member was not found")))
                            .flatMap(targetMember -> {
                                if (targetMember.getMemberRole() == MemberRole.ADMIN) {
                                    return Mono.error(new AppException(
                                            ErrorCode.CHAT_ADMIN_REQUIRED,
                                            "An admin cannot remove another admin"));
                                }
                                return memberRepository.markRemoved(
                                                id, target, conversation.getLastMessageSeq(), Instant.now())
                                        .flatMap(updated -> updated > 0
                                                ? systemMessageService.insert(
                                                                id, actor, SystemMessageAction.MEMBER_REMOVED, target, null)
                                                        .map(system -> MemberOutcome.system(
                                                                new MemberMutationResponse(id, target, "REMOVED", null),
                                                                system,
                                                                ChatEvent.memberRemoved(id, actor, target)))
                                                : Mono.error(new AppException(
                                                        ErrorCode.CHAT_MEMBER_UPDATE_FAILED,
                                                        "Remove group member failed")));
                            });
                });
        return transactionalOperator.transactional(work).flatMap(this::publishMemberOutcome);
    }

    public Mono<MemberMutationResponse> changeMemberRole(
            String actorId, String conversationId, String targetUserId, ChangeMemberRoleRequest request
    ) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        String target = requireIdentifier(targetUserId, "targetUserId");
        if (request == null || request.role() == null) {
            return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Member role is required"));
        }
        Mono<MemberOutcome> work = requireAdminForUpdate(id, actor)
                .flatMap(conversation -> {
                    ensureWritable(conversation);
                    return memberRepository.findMembershipForUpdate(id, target)
                            .filter(member -> member.getMemberStatus() == MemberStatus.ACTIVE)
                            .switchIfEmpty(Mono.error(new AppException(ErrorCode.CHAT_MEMBER_NOT_FOUND, "Active target member was not found")))
                            .flatMap(targetMember -> {
                                if (targetMember.getMemberRole() == request.role()) {
                                    return Mono.just(MemberOutcome.plain(new MemberMutationResponse(
                                            id, target, "ROLE_UNCHANGED", null)));
                                }
                                if (targetMember.getMemberRole() == MemberRole.ADMIN
                                        && request.role() == MemberRole.USER
                                        && !actor.equals(target)) {
                                    return Mono.error(new AppException(
                                            ErrorCode.CHAT_ADMIN_REQUIRED,
                                            "An admin cannot demote another admin"));
                                }
                                Mono<Void> guard = request.role() == MemberRole.USER
                                        ? ensureAdminCanExit(id, targetMember)
                                        : Mono.empty();
                                return guard.then(memberRepository.updateRole(id, target, request.role()))
                                        .flatMap(updated -> updated > 0
                                                ? systemMessageService.insert(id, actor, SystemMessageAction.MEMBER_ROLE_CHANGED,
                                                                target, request.role().name())
                                                        .map(system -> MemberOutcome.system(new MemberMutationResponse(
                                                                id, target, "ROLE_CHANGED", null), system))
                                                : Mono.error(new AppException(
                                                        ErrorCode.CHAT_MEMBER_UPDATE_FAILED, "Change member role failed")));
                            });
                });
        return transactionalOperator.transactional(work).flatMap(this::publishMemberOutcome);
    }

    public Mono<MemberMutationResponse> leaveConversation(String actorId, String conversationId) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        Mono<MemberOutcome> work = accessService.requireActiveMember(id, actor)
                .flatMap(actorMember -> conversationRepository.findByIdForUpdate(id)
                        .switchIfEmpty(Mono.error(new AppException(ErrorCode.CONVERSATION_NOT_FOUND, "Conversation not found")))
                        .flatMap(conversation -> {
                            if (conversation.getConversationType() != ConversationType.GROUP) {
                                return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Only group members can leave a conversation"));
                            }
                            Mono<Void> guard = conversation.isDissolved()
                                    ? Mono.empty()
                                    : ensureAdminCanExit(id, actorMember);
                            if (conversation.isDissolved()) {
                                return guard.then(memberRepository.markLeft(
                                                id, actor, conversation.getLastMessageSeq(), Instant.now()))
                                        .flatMap(updated -> updated > 0
                                                ? Mono.just(MemberOutcome.plain(new MemberMutationResponse(id, actor, "LEFT", null)))
                                                : Mono.error(new AppException(ErrorCode.CHAT_MEMBER_UPDATE_FAILED, "Leave group failed")));
                            }
                            return guard.then(systemMessageService.insert(
                                            id, actor, SystemMessageAction.MEMBER_LEFT, actor, null))
                                    .flatMap(system -> memberRepository.markLeft(
                                                    id, actor, conversation.getLastMessageSeq() + 1L, Instant.now())
                                            .flatMap(updated -> updated > 0
                                                    ? Mono.just(MemberOutcome.system(
                                                            new MemberMutationResponse(id, actor, "LEFT", null), system))
                                                    : Mono.error(new AppException(ErrorCode.CHAT_MEMBER_UPDATE_FAILED, "Leave group failed"))));
                        }));
        return transactionalOperator.transactional(work).flatMap(this::publishMemberOutcome);
    }

    private Mono<Conversation> requireAdminForUpdate(String conversationId, String actorId) {
        return conversationRepository.findByIdForUpdate(conversationId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CONVERSATION_NOT_FOUND, "Conversation not found")))
                .filter(conversation -> conversation.getConversationType() == ConversationType.GROUP)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CHAT_ADMIN_REQUIRED, "Group conversation is required")))
                .flatMap(conversation -> memberRepository.findMembershipForUpdate(conversationId, actorId)
                        .filter(member -> member.getMemberStatus() == MemberStatus.ACTIVE
                                && member.getMemberRole() == MemberRole.ADMIN)
                        .switchIfEmpty(Mono.error(new AppException(
                                ErrorCode.CHAT_ADMIN_REQUIRED, "Active group admin membership is required")))
                        .thenReturn(conversation));
    }

    private Mono<Void> ensureAdminCanExit(String conversationId, ConversationMember member) {
        if (member.getMemberRole() != MemberRole.ADMIN) return Mono.empty();
        return memberRepository.countActiveAdmins(conversationId)
                .flatMap(count -> count != null && count > 1
                        ? Mono.empty()
                        : Mono.error(new AppException(
                                ErrorCode.CHAT_LAST_ADMIN_CANNOT_LEAVE, "The last active admin cannot be removed or demoted")));
    }
    private void ensureWritable(Conversation conversation) {
        if (conversation.isDissolved()) {
            throw new AppException(ErrorCode.CHAT_CONVERSATION_DISSOLVED, "The group conversation is read-only after dissolution");
        }
    }

    private Mono<MemberMutationResponse> activateMember(
            String conversationId,
            String targetUserId,
            long joinedSequence,
            ConversationMember existing
    ) {
        if (existing == null) return addActiveMember(conversationId, targetUserId, joinedSequence);
        return memberRepository.reactivateMember(
                        conversationId, targetUserId, joinedSequence, joinedSequence - 1L, Instant.now())
                .flatMap(updated -> updated > 0
                        ? Mono.just(new MemberMutationResponse(conversationId, targetUserId, "REACTIVATED", null))
                        : Mono.error(new AppException(ErrorCode.CHAT_MEMBER_UPDATE_FAILED, "Reactivate chat member failed")));
    }

    private Mono<MemberMutationResponse> addActiveMember(String conversationId, String targetUserId, long joinedSequence) {
        Instant now = Instant.now();
        ConversationMember member = ConversationMember.builder()
                .id(UUID.randomUUID().toString()).conversationId(conversationId).userId(targetUserId)
                .memberRole(MemberRole.USER).memberStatus(MemberStatus.ACTIVE)
                .joinedSeq(joinedSequence).lastDeliveredSeq(joinedSequence - 1L)
                .lastReadSeq(joinedSequence - 1L).joinedAt(now).build();
        return r2dbcEntityTemplate.insert(ConversationMember.class).using(member)
                .map(saved -> new MemberMutationResponse(conversationId, targetUserId, "ADDED", null))
                .onErrorMap(DataIntegrityViolationException.class, error -> new AppException(
                        ErrorCode.CHAT_MEMBER_ALREADY_EXISTS, "Target user is already a member", error));
    }

    private Mono<MemberOutcome> createPendingOutcome(String conversationId, String actorId, String targetUserId) {
        return Mono.zip(
                        createPendingRequest(conversationId, actorId, targetUserId),
                        memberRepository.findActiveAdminUserIds(conversationId).collectList())
                .map(tuple -> MemberOutcome.request(tuple.getT1(), ChatEvent.memberRequested(
                        conversationId,
                        actorId,
                        tuple.getT1().requestId(),
                        targetUserId,
                        tuple.getT2())));
    }
    private Mono<MemberMutationResponse> createPendingRequest(String conversationId, String actorId, String targetUserId) {
        Instant now = Instant.now();
        ConversationMemberRequest request = ConversationMemberRequest.builder()
                .id(UUID.randomUUID().toString()).conversationId(conversationId).targetUserId(targetUserId)
                .requestedBy(actorId).requestStatus(MemberRequestStatus.PENDING)
                .pendingKey(pendingKey(conversationId, targetUserId)).createdAt(now).build();
        return r2dbcEntityTemplate.insert(ConversationMemberRequest.class).using(request)
                .map(saved -> new MemberMutationResponse(conversationId, targetUserId, "PENDING", saved.getId()))
                .onErrorMap(DataIntegrityViolationException.class, error -> new AppException(
                        ErrorCode.CHAT_MEMBER_REQUEST_ALREADY_PENDING, "A pending member request already exists", error));
    }

    private Mono<MemberMutationResponse> resolveApprove(String actorId, ConversationMemberRequest request, long joinedSequence) {
        ensurePending(request);
        return memberRepository.findMembershipForUpdate(request.getConversationId(), request.getTargetUserId())
                .flatMap(existing -> existing.getMemberStatus() == MemberStatus.ACTIVE
                        ? Mono.<MemberMutationResponse>error(new AppException(
                                ErrorCode.CHAT_MEMBER_ALREADY_EXISTS, "Target user is already an active member"))
                        : existing.getMemberStatus() == MemberStatus.BANNED
                                ? Mono.<MemberMutationResponse>error(new AppException(
                                        ErrorCode.CONVERSATION_FORBIDDEN, "Banned member cannot be re-added"))
                                : activateMember(request.getConversationId(), request.getTargetUserId(), joinedSequence, existing))
                .switchIfEmpty(activateMember(request.getConversationId(), request.getTargetUserId(), joinedSequence, null))
                .flatMap(response -> {
                    request.setRequestStatus(MemberRequestStatus.APPROVED);
                    request.setResolvedBy(actorId);
                    request.setResolvedAt(Instant.now());
                    request.setPendingKey(null);
                    return requestRepository.save(request).thenReturn(new MemberMutationResponse(
                            request.getConversationId(), request.getTargetUserId(), "APPROVED", request.getId()));
                });
    }
    private Mono<MemberMutationResponse> resolveReject(String actorId, ConversationMemberRequest request) {
        ensurePending(request);
        request.setRequestStatus(MemberRequestStatus.REJECTED);
        request.setResolvedBy(actorId);
        request.setResolvedAt(Instant.now());
        request.setPendingKey(null);
        return requestRepository.save(request).map(saved -> new MemberMutationResponse(
                saved.getConversationId(), saved.getTargetUserId(), "REJECTED", saved.getId()));
    }

    private Mono<MemberMutationResponse> publishMemberOutcome(MemberOutcome outcome) {
        Mono<Void> publishSystem = outcome.system() == null
                ? Mono.empty()
                : publishSystem(outcome.system());
        Mono<Void> publishEvent = publishEvent(outcome.event(), "publishMemberRequest");
        Mono<Void> publishLifecycle = publishEvent(outcome.lifecycleEvent(), "publishMembershipLifecycle");
        return Mono.when(publishSystem, publishEvent, publishLifecycle)
                .thenReturn(outcome.response());
    }

    private Mono<Void> publishEvent(ChatEvent event, String operation) {
        if (event == null) return Mono.empty();
        return eventPublisher.publish(event)
                .onErrorResume(error -> {
                    log.error("|ConversationMemberService|{}|failed|conversationId={}|error={}",
                            operation, event.conversationId(), error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> publishSystem(ChatSystemMessageService.SystemMessageResult system) {
        return systemMessageService.publish(system)
                .onErrorResume(error -> {
                    log.error("|ConversationMemberService|publishSystem|failed|conversationId={}|error={}",
                            system.message().conversationId(), error.getMessage());
                    return Mono.empty();
                });
    }

    private void ensurePending(ConversationMemberRequest request) {
        if (request.getRequestStatus() != MemberRequestStatus.PENDING) {
            throw new AppException(ErrorCode.CHAT_MEMBER_REQUEST_ALREADY_RESOLVED, "Member request is already resolved");
        }
    }

    private Mono<Void> ensureUserExists(String userId) {
        return userAccountQueryService.exists(userId).flatMap(exists -> Boolean.TRUE.equals(exists)
                ? Mono.empty() : Mono.error(new AppException(ErrorCode.USER_NOT_FOUND, "Target user was not found")));
    }

    private String requireIdentifier(String value, String name) {
        if (value == null || value.isBlank()) throw new AppException(ErrorCode.CHAT_REQUEST_INVALID, name + " is required");
        return value.trim();
    }

    private String pendingKey(String conversationId, String targetUserId) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest((conversationId + ":" + targetUserId).getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    private record MemberOutcome(
            MemberMutationResponse response,
            ChatSystemMessageService.SystemMessageResult system,
            ChatEvent event,
            ChatEvent lifecycleEvent
    ) {
        static MemberOutcome system(MemberMutationResponse response, ChatSystemMessageService.SystemMessageResult system) {
            return new MemberOutcome(response, system, null, null);
        }
        static MemberOutcome system(
                MemberMutationResponse response,
                ChatSystemMessageService.SystemMessageResult system,
                ChatEvent lifecycleEvent
        ) {
            return new MemberOutcome(response, system, null, lifecycleEvent);
        }
        static MemberOutcome request(MemberMutationResponse response, ChatEvent event) {
            return new MemberOutcome(response, null, event, null);
        }
        static MemberOutcome plain(MemberMutationResponse response) {
            return new MemberOutcome(response, null, null, null);
        }
    }

    private record NicknameOutcome(MemberNicknameResponse response, ChatSystemMessageService.SystemMessageResult system) {
    }
}