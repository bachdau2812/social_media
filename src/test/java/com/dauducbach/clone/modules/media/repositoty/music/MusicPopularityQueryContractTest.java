package com.dauducbach.clone.modules.media.repositoty.music;

import com.dauducbach.clone.modules.media.entity.music.Musics;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class MusicPopularityQueryContractTest {
    private static final Path REPOSITORY_SOURCE = Path.of(
            "src",
            "main",
            "java",
            "com",
            "dauducbach",
            "clone",
            "modules",
            "media",
            "repositoty",
            "music",
            "MusicsRepository.java");

    @Test
    void mapsPopularityAsBigDecimal() {
        assertThat(Arrays.stream(Musics.class.getDeclaredFields())
                        .filter(field -> field.getName().equals("popularity")))
                .singleElement()
                .extracting(Field::getType)
                .isEqualTo(BigDecimal.class);
    }

    @Test
    void ordersEveryListQueryOnlyByPopularityDescending() throws Exception {
        String source = Files.readString(REPOSITORY_SOURCE);
        long popularityOrderCount = Pattern.compile("ORDER BY\\s+popularity\\s+DESC")
                .matcher(source)
                .results()
                .count();

        assertThat(popularityOrderCount).isEqualTo(4L);
        assertThat(source).doesNotContain("ORDER BY display_name ASC");
        assertThat(source).doesNotContain("ORDER BY popularity DESC,");
    }
}
