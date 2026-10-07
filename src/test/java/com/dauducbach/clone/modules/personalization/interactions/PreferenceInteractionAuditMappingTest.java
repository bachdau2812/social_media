package com.dauducbach.clone.modules.personalization.interactions;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceInteraction;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class PreferenceInteractionAuditMappingTest {

    @Test
    void mapsCanonicalCommentToAuditContractWithoutLeakingFeedDto() {
        Instant occurredAt = Instant.parse("2026-10-01T12:30:00Z");
        PreferenceInteraction event = new PreferenceInteraction(
                "event-1", "user-1", "post-1", "COMMENT", "comment-1", occurredAt);

        AuditEntry audit = PreferenceInteractionProcessingService.toAuditEntry(event);

        assertThat(audit.actorId()).isEqualTo("user-1");
        assertThat(audit.action()).isEqualTo(AuditActionType.COMMENT_POST);
        assertThat(audit.resourceType()).isEqualTo(EntityType.POST.name());
        assertThat(audit.resourceId()).isEqualTo("post-1");
        assertThat(audit.sourceEventId()).isEqualTo("event-1");
        assertThat(GsonUtils.fromString(audit.metadataJson()).get("sourceId").getAsString()).isEqualTo("comment-1");
        assertThat(GsonUtils.fromString(audit.metadataJson()).get("occurredAt").getAsString())
                .isEqualTo(occurredAt.toString());
    }
}
