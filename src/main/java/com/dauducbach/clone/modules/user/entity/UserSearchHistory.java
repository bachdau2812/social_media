package com.dauducbach.clone.modules.user.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder
@Table("user_search_histories")
public class UserSearchHistory {
    @Id
    String id;
    String userId;
    String keyword;
    String normalizedKeyword;
    Long searchCount;
    Instant createdAt;
    Instant lastSearchedAt;
}
