package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.ReactionType;
import java.util.List;

public record ReactionSnapshot(String messageId, long messageSeq, long reactionVersion,
                               ReactionType myReaction, boolean isReact, long likeCount,
                               List<ReactionCount> reactions) {
    public ReactionSnapshot { reactions = List.copyOf(reactions); }

    public ReactionState state(String actorId) {
        return new ReactionState(messageId, messageSeq, reactionVersion, actorId, myReaction, likeCount, reactions);
    }
}
