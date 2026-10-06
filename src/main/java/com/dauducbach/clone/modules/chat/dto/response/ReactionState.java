package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.ReactionType;
import java.util.List;

public record ReactionState(String messageId, long messageSeq, long reactionVersion,
                            String actorId, ReactionType reaction, long likeCount, List<ReactionCount> reactions) {
    public ReactionState { reactions = List.copyOf(reactions); }
}
