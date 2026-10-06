package com.dauducbach.clone.modules.user.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.relational.core.mapping.Table;
import java.time.Instant;

@Data
@Table("user_vector_update_operations")
public class UserVectorUpdateOperation {
    @Id private Long id;
    @Version private Long rowVersion;
    private String operationKey;
    private String userId;
    private String operationKind;
    private String baselineVector;
    private String desiredVector;
    private Long baselineSeqNo;
    private Long baselinePrimaryTerm;
    private String operationId;
    private String formulaVersion;
    private Instant rangeFrom;
    private Instant rangeTo;
    private String status;
    private Instant createdAt;
    private Instant updatedAt;
}
