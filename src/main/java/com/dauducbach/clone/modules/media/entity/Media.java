package com.dauducbach.clone.modules.media.entity;

import com.dauducbach.clone.modules.media.constant.OwnerType;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Builder

@Table("media")
public class Media {
    @Id
    String assetId;
    String publicId;
    int width;
    int height;
    String mediaFormat;
    String resourceType;
    int bytes;
    String url;
    String secureUrl;
    String ownerId;
    OwnerType ownerType;
    String version;
    String versionId;
    String displayName;     // Danh cho audio(story)
    Instant createdAt;
    Instant updatedAt;
}
