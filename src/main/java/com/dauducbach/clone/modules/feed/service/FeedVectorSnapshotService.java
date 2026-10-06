package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.infrastructure.vector.VectorMath;
import com.dauducbach.clone.modules.feed.dto.FeedVectorSnapshot;
import com.dauducbach.clone.modules.user.dto.UserVectorSnapshot;
import com.dauducbach.clone.modules.user.service.UserVectorSnapshotService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.util.List;

@Service
@RequiredArgsConstructor
public class FeedVectorSnapshotService {
    private final UserVectorSnapshotService snapshots;

    public Mono<FeedVectorSnapshot> load(String userId) {
        return snapshots.load(userId).map(snapshot -> new FeedVectorSnapshot(snapshot.version(), queryVector(snapshot)));
    }

    private List<Double> queryVector(UserVectorSnapshot snapshot) {
        List<Double> longTerm = snapshot.longTerm().isEmpty() ? snapshot.profile() : snapshot.longTerm();
        List<Double> shortTerm = snapshot.shortTerm().isEmpty() ? snapshot.profile() : snapshot.shortTerm();
        if (longTerm.isEmpty()) return shortTerm;
        if (shortTerm.isEmpty()) return longTerm;
        try {
            return VectorMath.mix(longTerm, snapshot.hasLearnedHistory() ? .7 : .3,
                    shortTerm, snapshot.hasLearnedHistory() ? .3 : .7);
        } catch (IllegalArgumentException cancelled) {
            // Inputs are already validated by the shared snapshot; opposite directions may cancel.
            return List.of();
        }
    }
}
