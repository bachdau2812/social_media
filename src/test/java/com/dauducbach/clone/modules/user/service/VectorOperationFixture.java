package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.infrastructure.vector.*;
import com.dauducbach.clone.modules.user.entity.*;
import com.dauducbach.clone.modules.user.repositoty.*;
import com.dauducbach.clone.utils.GsonUtils;
import org.springframework.data.elasticsearch.core.query.SeqNoPrimaryTerm;
import org.springframework.dao.OptimisticLockingFailureException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class VectorOperationFixture {
    final UserVectorUpdateOperationRepository repository = mock(UserVectorUpdateOperationRepository.class);
    final UserDetailsRepository users = mock(UserDetailsRepository.class);
    final UserVectorQueryService query = mock(UserVectorQueryService.class);
    final UserVectorStore store = mock(UserVectorStore.class);
    final InMemoryVectorRedisState redis = new InMemoryVectorRedisState();
    final UserVectorCoordinator coordinator = new UserVectorCoordinator(redis);
    final UserVectorOperationService service = new UserVectorOperationService(repository, users, query, store, coordinator, redis);
    final Map<String, UserVectorUpdateOperation> journal = new LinkedHashMap<>();
    UserDetailVector document;
    boolean userExists = true;
    boolean failCompletion;
    boolean failAfterEs;
    boolean failApplied;
    boolean hangAfterEs;
    int esWrites;
    VectorOperationFixture() {
        when(users.existsById("u")).thenAnswer(call -> Mono.fromSupplier(() -> userExists));
        when(query.getSnapshot("u")).thenAnswer(call -> Mono.defer(() -> Mono.justOrEmpty(copyDoc(document))));
        when(repository.findPending("u")).thenAnswer(call -> Flux.defer(() -> Flux.fromIterable(journal.values())
                .filter(op -> List.of("PREPARED", "ES_APPLIED").contains(op.getStatus())).map(this::copy)));
        when(repository.findByOperationKey(anyString())).thenAnswer(call -> Mono.defer(() -> Mono.justOrEmpty(journal.get(call.getArgument(0))).map(this::copy)));
        when(repository.save(any())).thenAnswer(call -> Mono.defer(() -> {
            UserVectorUpdateOperation op = copy(call.getArgument(0));
            if (failCompletion && op.getStatus().equals("COMPLETED")) return Mono.error(new IllegalStateException("SQL unavailable"));
            if (failApplied && op.getStatus().equals("ES_APPLIED")) return Mono.error(new IllegalStateException("SQL unavailable after ES"));
            if (op.getId() == null) {
                op.setId((long) journal.size() + 1); op.setRowVersion(0L);
            } else {
                var durable = journal.get(op.getOperationKey());
                if (durable == null || !Objects.equals(durable.getId(), op.getId())
                        || !Objects.equals(durable.getRowVersion(), op.getRowVersion()))
                    return Mono.error(new OptimisticLockingFailureException("Stale journal row"));
                op.setRowVersion(op.getRowVersion() + 1);
            }
            journal.put(op.getOperationKey(), copy(op)); return Mono.just(copy(op));
        }));
        when(store.upsertProfileAndSeedLongTerm(eq("u"), anyList(), anyString(), nullable(Long.class), nullable(Long.class)))
                .thenAnswer(call -> Mono.defer(() -> {
                    assertPrepared(call.getArgument(2));
                    Long seq = call.getArgument(3);
                    if (document != null && (seq == null || seq != document.getSeqNoPrimaryTerm().sequenceNumber())) return Mono.error(conflict());
                    if (document == null) document = new UserDetailVector();
                    document.setUserVector(call.getArgument(1)); document.setUserProfileOperationId(call.getArgument(2));
                    document.setUserVectorModel(VectorMath.MODEL); document.setUserVectorDimension(768); document.setUserVectorSchemaVersion(1);
                    if (document.getUserLongTermVector() == null || document.getUserLongTermVector().isEmpty()) {
                        document.setUserLongTermVector(call.getArgument(1)); document.setUserLongTermOperationId(call.getArgument(2));
                        document.setUserLongTermVectorModel(VectorMath.MODEL); document.setUserLongTermVectorDimension(768);
                        document.setUserLongTermVectorSchemaVersion(1); document.setHasLearnedHistory(false);
                    }
                    document.setSeqNoPrimaryTerm(new SeqNoPrimaryTerm(seq == null ? 0 : seq + 1, 1)); esWrites++;
                    if (hangAfterEs) return Mono.never();
                    return failAfterEs ? Mono.error(new IllegalStateException("response lost")) : Mono.empty();
                }));
        when(store.updateLongTerm(eq("u"), anyList(), anyString(), anyLong(), anyLong()))
                .thenAnswer(call -> Mono.defer(() -> {
                    assertPrepared(call.getArgument(2));
                    long seq = call.getArgument(3);
                    if (document == null || seq != document.getSeqNoPrimaryTerm().sequenceNumber()) return Mono.error(conflict());
                    document.setUserLongTermVector(call.getArgument(1)); document.setUserLongTermOperationId(call.getArgument(2));
                    document.setHasLearnedHistory(true); document.setSeqNoPrimaryTerm(new SeqNoPrimaryTerm(seq + 1, 1)); esWrites++;
                    return Mono.empty();
                }));
    }
    void assertPrepared(String id) {
        if (journal.values().stream().noneMatch(op -> op.getOperationId().equals(id) && op.getStatus().equals("PREPARED")))
            throw new AssertionError("ES write without durable PREPARED operation");
    }
    UserVectorUpdateOperation copy(UserVectorUpdateOperation op) {
        if (op == null) return null;
        // Gson Instant is not reflective-safe on JDK21; copy the entity's ordinary fields explicitly.
        var result = new UserVectorUpdateOperation();
        result.setId(op.getId()); result.setRowVersion(op.getRowVersion()); result.setUserId(op.getUserId()); result.setOperationKey(op.getOperationKey());
        result.setOperationKind(op.getOperationKind()); result.setOperationId(op.getOperationId()); result.setStatus(op.getStatus());
        result.setBaselineVector(op.getBaselineVector()); result.setDesiredVector(op.getDesiredVector());
        result.setBaselineSeqNo(op.getBaselineSeqNo()); result.setBaselinePrimaryTerm(op.getBaselinePrimaryTerm());
        result.setFormulaVersion(op.getFormulaVersion()); result.setRangeFrom(op.getRangeFrom()); result.setRangeTo(op.getRangeTo());
        result.setCreatedAt(op.getCreatedAt()); result.setUpdatedAt(op.getUpdatedAt()); return result;
    }
    static UserDetailVector copyDoc(UserDetailVector doc) {
        if (doc == null) return null;
        var copy = new UserDetailVector(); org.springframework.beans.BeanUtils.copyProperties(doc, copy);
        if (doc.getUserVector() != null) copy.setUserVector(new ArrayList<>(doc.getUserVector()));
        if (doc.getUserLongTermVector() != null) copy.setUserLongTermVector(new ArrayList<>(doc.getUserLongTermVector()));
        return copy;
    }
    static List<Double> vector(int coordinate) { var list = new ArrayList<>(Collections.nCopies(768, 0d)); list.set(coordinate, 1d); return list; }
    UserVectorStore.RetryableVectorConflictException conflict() { return new UserVectorStore.RetryableVectorConflictException(new IllegalStateException("OCC")); }
    Mono<UserVectorUpdateOperation> profile(String key, int coordinate) {
        return coordinator.withUserLock("u", lease -> service.applyProfile(lease, key, vector(coordinate)));
    }
}
