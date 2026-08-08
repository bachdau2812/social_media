package com.dauducbach.clone.modules.media.entity.music;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder
@Table("musics")
public class Musics {
    @Id
    String id;
    String slugName;
    String displayName;
    String descriptions;
    String displayImages;
    String singleName;
    String songUrl;
    Long duration;
    String category;
    Short releaseYear;
    String albumName;
    Boolean fetched;
}
