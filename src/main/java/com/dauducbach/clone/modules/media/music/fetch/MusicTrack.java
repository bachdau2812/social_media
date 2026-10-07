package com.dauducbach.clone.modules.media.music.fetch;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

/** Business-facing music data used by the fetch workflow; persistence annotations stay in the adapter. */
@Data
@Builder(toBuilder = true)
public class MusicTrack {
    private String id;
    private String slugName;
    private String displayName;
    private String descriptions;
    private String displayImages;
    private String singleName;
    private String songUrl;
    private Long duration;
    private String category;
    private Short releaseYear;
    private String albumName;
    private Boolean fetched;
    private BigDecimal popularity;
}
