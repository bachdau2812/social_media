package com.dauducbach.clone.modules.feed.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;
import java.time.Instant;

@Data
@Table("feed_interaction_processing")
public class FeedInteractionProcessing {
    @Id private String sourceEventId;
    @Version private Long rowVersion;
    private String userId;
    private String postId;
    private String action;
    private String sourceId;
    private Instant occurredAt;
    private String canonicalTopic;
    private String canonicalGeneration;
    private Integer canonicalPartition;
    private Long canonicalOffset;
    private String status;
    private String operationId;
    private String desiredShortVector;
    private Instant vectorExpiresAt;
    private String reason;
    private Instant createdAt;
    private Instant updatedAt;
}
